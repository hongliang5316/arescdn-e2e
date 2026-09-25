#!/bin/bash
# AresCDN 核心功能测试
#
# 在控制面机器上执行(先执行过 setup.sh): sudo ./run-tests.sh [组...]
#   basic    回源链路 / 缓存规则 / 大文件与 Range / 刷新 / 预热
#   follow   302 跟随
#   shard    分片
#   shard302 分片 + 302 跟随
#   share    共享缓存域名(SHARE_HOST 共享 HOST 的缓存)
#   不带参数时执行全部.
#
# 链路: 本机 curl -> 边缘层节点(EDGE) -> 回源层节点(ORIGIN_LAYER_IP) -> 本机测试源站 :8081
# 环境配置见 lab.env; 测试域名组的配置通过 tools/cdnapi.sh 调用 API 修改,
# 每组结束时恢复本组改过的配置, 退出时 302 跟随、分片关闭, 共享缓存域名恢复为 HOST.
set -u

BASE=$(cd "$(dirname "$0")" && pwd)
if [ ! -f "$BASE/lab.env" ]; then
	echo "缺少 lab.env, 先执行 sudo ./setup.sh"
	exit 2
fi
set -a
. "$BASE/lab.env"
set +a
if [ -z "$DG" ] || [ -z "$SHARE_DG" ]; then
	echo "lab.env 里没有域名组 ID, 先执行 sudo ./setup.sh"
	exit 2
fi

API=$BASE/tools/cdnapi.sh
DIR=$BASE/origin
LOG=$DIR/logs/access.log
RUN=$(date +%Y%m%d%H%M%S)
TEST_GROUPS=${*:-basic follow shard shard302 share}
T=$(mktemp -d)

PASS=0
FAIL=0
FAILED=()
G='\033[32m'
R='\033[31m'
Y='\033[33m'
B='\033[1m'
N='\033[0m'

section() { echo; echo -e "${B}== $*${N}"; }
info() { echo -e "  ${Y}INFO${N} $*"; }

# fetch PATH [curl 参数...]: 状态码在 $CODE, 响应头/体在 $T/hdr, $T/body
fetch() {
	local path=$1
	shift
	: >$T/hdr
	: >$T/body
	curl -s -m 60 -o $T/body -D $T/hdr -H "Host: $HOST" "$@" "http://$EDGE$path"
	CODE=$(awk 'NR==1{print $2}' $T/hdr)
}
hdr() { grep -i "^$1:" $T/hdr | head -1 | cut -d' ' -f2- | tr -d '\r'; }
body() { cat $T/body; }
body_md5() { md5sum <$T/body | cut -d' ' -f1; }
file_md5() { md5sum <"$1" | cut -d' ' -f1; }
# slice_md5 文件 起点 长度
slice_md5() { tail -c +$(($2 + 1)) "$1" | head -c $3 | md5sum | cut -d' ' -f1; }
jget() { jq -r "$1 // \"<无>\"" $T/body 2>/dev/null || echo "<非JSON>"; }

# HEAD 响应头之后的字节数: curl -I 会把响应头写进 -o 文件, 所以用原始 socket 检查
raw_head_body_len() {
	python3 - "$EDGE" "$HOST" "$1" <<'EOF'
import socket, sys
edge, host, path = sys.argv[1:4]
s = socket.create_connection((edge, 80), timeout=10)
s.sendall(f"HEAD {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
data = b""
while True:
    try:
        chunk = s.recv(65536)
    except socket.timeout:
        break
    if not chunk:
        break
    data += chunk
print(len(data.partition(b"\r\n\r\n")[2]))
EOF
}

# httpbin /range/N 的内容是 a-z 循环: pattern_ok 起点 长度
pattern_ok() {
	python3 - "$T/body" "$1" "$2" <<'EOF'
import sys
d = open(sys.argv[1], "rb").read()
off, n = int(sys.argv[2]), int(sys.argv[3])
ok = len(d) == n and all(b == 97 + (off + i) % 26 for i, b in enumerate(d))
print("ok" if ok else "len=%d head=%r" % (len(d), d[:20]))
EOF
}

# check 描述 实际值 期望值(可用 glob, 如 "abc *")
check() {
	local desc=$1 actual=$2 expect=$3
	if [[ "$actual" == $expect ]]; then
		PASS=$((PASS + 1))
		echo -e "  ${G}PASS${N} $desc"
	else
		FAIL=$((FAIL + 1))
		FAILED+=("$desc")
		echo -e "  ${R}FAIL${N} $desc: 期望 [$expect], 实际 [$actual]"
	fi
}

# 源站访问日志: mark 记下当前位置, 之后的请求用 hits / ranges 统计
mark() { MARK=$(wc -l <$LOG); }
since_mark() { tail -n +$((MARK + 1)) $LOG; }
hits() { since_mark | grep -cF -- "$1"; }
# ranges 模式: mark 之后匹配请求带的 Range 头(排序, 每行一个)
ranges() { since_mark | grep -F -- "$1" | grep -o 'range="[^"]*"' | sed 's/^range="//; s/"$//' | sort; }
# expect_chunks 文件大小 分片大小 [首片 末片]: 期望的分片 Range(排序), 尾片按文件大小截断
expect_chunks() {
	local size=$1 cs=$2 first=${3:-0} last=${4:-$(((${1} + ${2} - 1) / ${2} - 1))} i s e
	for ((i = first; i <= last; i++)); do
		s=$((i * cs))
		e=$(((i + 1) * cs - 1))
		((e >= size)) && e=$((size - 1))
		echo "bytes=$s-$e"
	done | sort
}
# same 实际 期望: 一致输出 ok, 否则输出差异
same() {
	if [ "$1" = "$2" ]; then
		echo ok
	else
		echo "差异(-实际 +期望): $(diff <(echo "$1") <(echo "$2") | grep '^[<>]' | head -4 | tr '\n' ' ')"
	fi
}

api() { $API "$@" | tee -a $T/api.log | grep -q '"errno":0' && echo ok || tail -1 $T/api.log; }
# submit_task refresh|preheat JSON: 提交刷新/预热, 输出任务的 request_id; 失败时输出接口返回
submit_task() {
	local resp
	resp=$($API POST "/api/cdn/v1/cache/$1" "$2")
	if [ "$(echo "$resp" | jq -r .errno)" = 0 ]; then
		echo "$resp" | jq -r .request_id
	else
		echo "$resp"
	fi
}
# task_status refresh|preheat request_id: 任务里各 URL 的状态(去重)
task_status() { $API GET "/api/cdn/v1/cache/$1?request_id=$2" | jq -r "[.data.$1_url_list[]?.status] | unique | join(\",\")"; }
task_is() { [ "$(task_status "$1" "$2")" = "$3" ]; }
# 刚提交的任务可能要 1 秒后才能查到: 任务的 create_time 精确到秒, 写入时 MySQL 会四舍五入,
# 而查询的截止时间是把当前时间截断到秒, 小数部分 >= 0.5 的任务在下一秒前都查不到
task_done() { WAIT_TRIES=${3:-15} wait_until "$1 任务完成" task_is "$1" "$2" Success; }
# refresh_order request_id: dispatcher 处理 URL 刷新时各节点完成的顺序, 如 "origin:lab02:Success edge:lab01:Success"
# (取自 dispatcher 的 debug 日志 "Refresh done: 请求, 域名, 任务, 层级, 节点, 状态")
refresh_order() {
	grep -h "Refresh done: $1" $(ls -t "$DISPATCHER_LOG_DIR"/* | head -1) |
		sed -E 's/.*Refresh done: [^,]+, [^,]+, [^,]+, ([a-z]+), ([^,]+), ([A-Za-z]+).*/\1:\2:\3/' | tr '\n' ' ' | sed 's/ $//'
}
UUID_GLOB="????????-????-????-????-????????????"
ORDER_OK="origin:$ORIGIN_NODE:Success edge:$EDGE_NODE:Success"

set_follow() { api PATCH /api/cdn/v1/domaingroups "{\"unique_id\":\"$DG\",\"acc_follow302_max\":$1}"; }
set_shard() { api PATCH /api/cdn/v1/domaingroups "{\"unique_id\":\"$DG\",\"acc_sharding_size_str\":\"$1\"}"; }

# wait_until 描述 命令...: 每 2 秒重试, 最多 WAIT_TRIES(默认 30)次
wait_until() {
	local desc=$1
	shift
	for _ in $(seq 1 ${WAIT_TRIES:-30}); do
		"$@" && return 0
		sleep 2
	done
	echo "  等待超时: $desc"
	return 1
}
# 每次带新的查询参数, 避免命中缓存
probe_code() { fetch "$1?probe=$RUN-$RANDOM"; [ "$CODE" = "$2" ]; }
body_is() { fetch "$1"; [ "$(body)" = "$2" ]; }
origin_hit() { [ "$(hits "$1")" -ge 1 ]; }
# 分片是否生效: 看源站收到的小文件回源请求带不带首分片 Range(关闭时 nginx 日志记为 "-")
shard_is() {
	local p="/static/small.txt?probe=$RUN-$RANDOM"
	fetch "$p"
	grep -F "GET $p " $LOG | tail -1 | grep -qF "range=\"$1\""
}

# edge 的域名配置有两级缓存: 每个 nginx worker 的 lrucache 和节点的 shared dict, TTL 都是 5 秒
# (arescdn-edge lua/meta.lua). 探测到新配置只说明某个 worker 已更新, 其它 worker 最多还要 10 秒
CONFIG_SETTLE_SECS=11

follow_to() {
	check "API 设置 acc_follow302_max=$1" "$(set_follow $1)" ok
	if [ "$1" = 0 ]; then
		wait_until "302 跟随关闭生效" probe_code /302/rel 302
	else
		wait_until "302 跟随开启生效" probe_code /302/rel 200
	fi
	sleep $CONFIG_SETTLE_SECS
}
shard_to() { # shard_to 0|512KB|1MB 期望的首分片 Range
	check "API 设置分片 ${1}" "$(set_shard $1)" ok
	wait_until "分片 $1 生效" shard_is "$2"
	sleep $CONFIG_SETTLE_SECS
}

# on_host 域名 命令...: 临时改用另一个域名发请求
on_host() {
	local saved=$HOST rc
	HOST=$1
	shift
	"$@"
	rc=$?
	HOST=$saved
	return $rc
}

set_as_domain() { api PATCH /api/cdn/v1/domaingroups "{\"unique_id\":\"$SHARE_DG\",\"as_domain\":\"$1\"}"; }
# 共享是否生效: 用 test 域名缓存一个新地址, 再用 share 域名请求, HIT 表示共享(on), MISS 表示不共享(off)
share_is() {
	local p="/dyn/share-probe?r=$RUN-$RANDOM"
	fetch "$p"
	on_host $SHARE_HOST fetch "$p"
	if [ "$1" = on ]; then
		[ "$(hdr X-Cache-Status)" = HIT ]
	else
		[ "$(hdr X-Cache-Status)" = MISS ]
	fi
}
share_to() { # share_to 共享缓存域名(空表示不共享) on|off
	check "API 设置共享缓存域名为 [$1]" "$(set_as_domain "$1")" ok
	wait_until "共享缓存域名 [$1] 生效" share_is "$2"
	sleep $CONFIG_SETTLE_SECS
}

cleanup() {
	set_follow 0 >/dev/null
	set_shard 0 >/dev/null
	set_as_domain $HOST >/dev/null
	rm -rf $T
}
trap cleanup EXIT

BIG=$DIR/static/10m.bin
BIGSZ=$(stat -c %s $BIG)
BIGMD5=$(file_md5 $BIG)
CS=524288 # 512KB

# range_check 描述 路径 Range 起点 长度 [源文件]
range_check() {
	local src=${6:-$BIG}
	local end=$(($4 + $5 - 1))
	fetch "$2" -H "Range: bytes=$3"
	check "$1: 206" "$CODE" 206
	check "$1: Content-Range" "$(hdr Content-Range)" "bytes $4-$end/$(stat -c %s $src)"
	check "$1: 内容一致" "$(body_md5)" "$(slice_md5 $src $4 $5)"
}

# ===========================================================================
test_basic() {
	section "回源链路"
	mark
	fetch "/?r=$RUN"
	check "GET / 返回 200" "$CODE" 200
	check "内容来自测试源站" "$(body)" "arescdn origin-test ok"
	local line
	line=$(since_mark | grep -F "GET /?r=$RUN " | head -1)
	check "源站收到 1 次回源" "$(hits "GET /?r=$RUN ")" 1
	check "回源请求来自回源层节点" "$(echo "$line" | awk '{print $2}')" "$ORIGIN_LAYER_IP"
	check "回源 Host 为加速域名" "$(echo "$line" | awk '{print $4}')" "\"$HOST\""

	section "缓存规则"
	mark
	fetch "/dyn/c?r=$RUN"
	local b1
	b1=$(body)
	check "/dyn/(缓存 1h) 首次 MISS" "$(hdr X-Cache-Status)" MISS
	fetch "/dyn/c?r=$RUN"
	check "/dyn/ 再次请求 HIT" "$(hdr X-Cache-Status)" HIT
	check "/dyn/ 命中时内容与首次一致" "$(body)" "$b1"
	check "/dyn/ 源站只被请求 1 次" "$(hits "GET /dyn/c?r=$RUN ")" 1

	mark
	fetch "/nocache/c?r=$RUN"
	fetch "/nocache/c?r=$RUN"
	check "/nocache/(no_cache) 第二次不是 HIT" "$(hdr X-Cache-Status)" "[!H]*"
	check "/nocache/ 每次都回源" "$(hits "GET /nocache/c?r=$RUN ")" 2

	mark
	fetch "/echo?r=$RUN"
	fetch "/echo?r=$RUN"
	check "没有缓存规则的路径每次都回源" "$(hits "GET /echo?r=$RUN ")" 2

	section "大文件与 Range"
	mark
	fetch "/static/10m.bin?r=$RUN"
	check "10MB 文件首次 200" "$CODE" 200
	check "10MB 文件首次内容一致(md5)" "$(body_md5)" "$BIGMD5"
	fetch "/static/10m.bin?r=$RUN"
	check "10MB 文件再次请求 HIT" "$(hdr X-Cache-Status)" HIT
	check "10MB 文件命中时内容一致(md5)" "$(body_md5)" "$BIGMD5"
	check "10MB 文件只回源 1 次" "$(hits "GET /static/10m.bin?r=$RUN ")" 1

	range_check "已缓存 Range 开头" "/static/10m.bin?r=$RUN" "0-99" 0 100
	range_check "已缓存 Range 中间" "/static/10m.bin?r=$RUN" "5000000-5000999" 5000000 1000
	range_check "已缓存 Range 末尾(-100)" "/static/10m.bin?r=$RUN" "-100" $((BIGSZ - 100)) 100
	range_check "未缓存时首个请求就是 Range" "/static/10m.bin?r=$RUN-range" "1048576-1048675" 1048576 100

	fetch "/static/small.txt?r=$RUN" -I
	check "HEAD 返回 200" "$CODE" 200
	check "HEAD Content-Length 正确" "$(hdr Content-Length)" 11
	check "HEAD 没有响应体" "$(raw_head_body_len "/static/small.txt?r=$RUN")" 0

	section "缓存刷新"
	local F=refresh-$RUN.txt
	echo "v1-$RUN" >$DIR/static/$F
	fetch "/static/$F"
	fetch "/static/$F"
	check "刷新前: 命中缓存 v1" "$(hdr X-Cache-Status) $(body)" "HIT v1-$RUN"
	echo "v2-$RUN" >$DIR/static/$F
	fetch "/static/$F"
	check "源站改为 v2 后仍返回缓存的 v1" "$(body)" "v1-$RUN"
	local id
	id=$(submit_task refresh "{\"type\":\"file\",\"data_list\":[\"http://$HOST/static/$F\"]}")
	check "提交 URL 刷新" "$id" "$UUID_GLOB"
	wait_until "URL 刷新生效" body_is "/static/$F" "v2-$RUN"
	check "URL 刷新后返回 v2" "$(body)" "v2-$RUN"
	task_done refresh "$id"
	check "URL 刷新任务状态 Success(dispatcher 已下发到全部节点)" "$(task_status refresh "$id")" Success
	check "dispatcher 先刷回源层($ORIGIN_NODE), 再刷边缘层($EDGE_NODE)" "$(refresh_order "$id")" "$ORDER_OK"
	info "刷新明细接口返回 $($API GET "/api/cdn/v1/cache/refresh_detail?request_id=$id" | jq -r .data.total_count) 条(明细表目前没有写入方)"

	local D=dir-$RUN
	mkdir -p $DIR/static/$D
	echo "v1-$RUN" >$DIR/static/$D/a.txt
	fetch "/static/$D/a.txt"
	fetch "/static/$D/a.txt"
	check "目录刷新前: 命中缓存 v1" "$(hdr X-Cache-Status) $(body)" "HIT v1-$RUN"
	echo "v2-$RUN" >$DIR/static/$D/a.txt
	id=$(submit_task refresh "{\"type\":\"directory\",\"data_list\":[\"http://$HOST/static/$D/\"]}")
	check "提交目录刷新" "$id" "$UUID_GLOB"
	task_done refresh "$id" 3
	check "目录刷新任务状态 Success(不经过 dispatcher, 更新配置里的版本号)" "$(task_status refresh "$id")" Success
	wait_until "目录刷新生效" body_is "/static/$D/a.txt" "v2-$RUN"
	check "目录刷新后返回 v2" "$(body)" "v2-$RUN"

	section "预热"
	F=preheat-$RUN.txt
	echo "preheat-$RUN" >$DIR/static/$F
	mark
	id=$(submit_task preheat "{\"data_list\":[\"http://$HOST/static/$F\"]}")
	check "提交预热" "$id" "$UUID_GLOB"
	if wait_until "预热回源" origin_hit "GET /static/$F "; then
		sleep 2
		fetch "/static/$F"
		check "预热后首次访问 HIT" "$(hdr X-Cache-Status)" HIT
		check "预热后访问不再回源" "$(hits "GET /static/$F ")" 1
		task_done preheat "$id"
		check "预热任务状态 Success" "$(task_status preheat "$id")" Success
	else
		check "预热触发回源" "源站未收到请求" "收到"
	fi
}

# ===========================================================================
test_follow() {
	section "302 跟随关闭时"
	follow_to 0
	fetch "/302/rel?r=$RUN-off"
	check "302 原样返回" "$CODE $(hdr Location)" "302 /dyn/rel-final"

	section "302 跟随(acc_follow302_max=3)"
	follow_to 3

	mark
	fetch "/302/rel?r=$RUN"
	check "相对地址: 200" "$CODE" 200
	check "相对地址: 返回跳转后的内容" "$(body)" "dyn uri=/dyn/rel-final *"
	fetch "/302/rel?r=$RUN"
	check "跟随结果被缓存: 再次请求 HIT" "$(hdr X-Cache-Status)" HIT
	check "跟随结果被缓存: 只回源 1 次" "$(hits "GET /302/rel?r=$RUN ")" 1

	fetch "/302/abs-self?r=$RUN"
	check "绝对地址(本域名): 200" "$CODE" 200
	check "绝对地址(本域名): 按源站配置回源" "$(body)" "dyn uri=/dyn/abs-self-final seq=* host=$HOST"

	fetch "/302/chain/3?r=$RUN"
	check "连续 3 跳(等于上限): 200" "$CODE $(body)" "200 dyn uri=/dyn/chain-final *"
	mark
	fetch "/302/chain/4?r=$RUN"
	check "连续 4 跳(超上限): 返回最后一跳的 302" "$CODE $(hdr Location)" "302 /dyn/chain-final"
	check "连续 4 跳: 源站收到 4 次请求" "$(hits 'GET /302/chain/')" 4

	mark
	fetch "/302/loop?r=$RUN"
	check "循环跳转: 返回 302" "$CODE" 302
	check "循环跳转: 回源 1+3 次后停止" "$(hits 'GET /302/loop')" 4

	fetch "/302/none?r=$RUN"
	check "302 没有 Location: 返回 302" "$CODE" 302
	check "302 没有 Location: 响应也没有 Location" "$(hdr Location)" ""

	mark
	fetch "/302/third-private?r=$RUN"
	check "跳到内网地址: 不跟随, 返回 302" "$CODE $(hdr Location)" "302 http://$ORIGIN_IP:8082/dyn/third"
	check "跳到内网地址: 没有访问内网" "$(hits ' 8082 "')" 0

	fetch "/302/to-echo?r=$RUN" -H "Cookie: sid=abc" -H "Authorization: Bearer t0k"
	check "同源跳转: 200" "$CODE" 200
	check "同源跳转: 携带 Cookie" "$(jget .cookie)" "sid=abc"
	check "同源跳转: 携带 Authorization" "$(jget .authorization)" "Bearer t0k"

	fetch "/302/third-public?r=$RUN" -H "Cookie: sid=abc" -H "Authorization: Bearer t0k"
	check "跳到外网第三方(httpbin.org): 200" "$CODE" 200
	check "第三方: Host 为 httpbin.org" "$(jget .headers.Host)" httpbin.org
	check "第三方: 不携带 Cookie" "$(jget .headers.Cookie)" "<无>"
	check "第三方: 不携带 Authorization" "$(jget .headers.Authorization)" "<无>"

	fetch "/301/rel?r=$RUN"
	check "301 不跟随" "$CODE $(hdr Location)" "301 /dyn/301-final"
	fetch "/307/rel?r=$RUN"
	check "307 不跟随" "$CODE $(hdr Location)" "307 /dyn/307-final"
	fetch "/302/rel?r=$RUN-post" -X POST -d a=1
	check "POST 不跟随" "$CODE" 302
	fetch "/302/rel?r=$RUN-head" -I
	check "HEAD 跟随: 200" "$CODE" 200

	section "跟随次数上限 5(acc_follow302_max=10)"
	check "API 设置 acc_follow302_max=10" "$(set_follow 10)" ok
	wait_until "acc_follow302_max=10 生效" probe_code /302/chain/4 200
	sleep $CONFIG_SETTLE_SECS
	fetch "/302/chain/5?r=$RUN"
	check "连续 5 跳: 200" "$CODE" 200
	fetch "/302/chain/6?r=$RUN"
	check "连续 6 跳: 按上限 5 处理, 返回 302" "$CODE $(hdr Location)" "302 /dyn/chain-final"

	follow_to 0
}

# ===========================================================================
test_shard() {
	section "分片 512KB: 整文件"
	shard_to 512KB "bytes=0-524287"

	local U="/static/10m.bin?r=$RUN-s1"
	mark
	fetch "$U"
	check "整文件: 200" "$CODE" 200
	check "整文件: Content-Length" "$(hdr Content-Length)" "$BIGSZ"
	check "整文件: 内容一致(md5)" "$(body_md5)" "$BIGMD5"
	check "整文件: 按 512KB 分 20 片回源, 区间正确且不重复" "$(same "$(ranges "GET $U ")" "$(expect_chunks $BIGSZ $CS)")" ok
	mark
	fetch "$U"
	check "再次请求: HIT" "$(hdr X-Cache-Status)" HIT
	check "再次请求: 内容一致" "$(body_md5)" "$BIGMD5"
	check "再次请求: 不回源" "$(hits "GET $U ")" 0
	range_check "已缓存, Range 跨分片边界" "$U" "524000-525000" 524000 1001
	check "已缓存 Range: 不回源" "$(hits "GET $U ")" 0

	section "分片 512KB: Range 只回源需要的分片"
	U="/static/10m.bin?r=$RUN-s2"
	mark
	range_check "未缓存, Range 在第 10 片内" "$U" "5000000-5000999" 5000000 1000
	check "只回源第 10 片" "$(same "$(ranges "GET $U ")" "$(expect_chunks $BIGSZ $CS 9 9)")" ok
	mark
	fetch "$U"
	check "部分缓存后请求整文件: 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
	check "部分缓存后请求整文件: X-Cache-Status 为 MISS(edge 只对外区分 HIT/MISS)" "$(hdr X-Cache-Status)" MISS
	check "部分缓存后请求整文件: 只回源缺的 19 片" "$(same "$(ranges "GET $U ")" "$( (expect_chunks $BIGSZ $CS 0 8; expect_chunks $BIGSZ $CS 10 19) | sort)")" ok

	U="/static/10m.bin?r=$RUN-s3"
	mark
	range_check "未缓存, Range 跨第 2、3 片" "$U" "1000000-1100000" 1000000 100001
	check "只回源第 2、3 片" "$(same "$(ranges "GET $U ")" "$(expect_chunks $BIGSZ $CS 1 2)")" ok

	U="/static/10m.bin?r=$RUN-s4"
	mark
	range_check "未缓存, Range 末尾(-1000)" "$U" "-1000" $((BIGSZ - 1000)) 1000
	check "末尾 Range: 先用 bytes=-1 探测大小, 再回源最后一片" "$(same "$(ranges "GET $U ")" "$( (echo bytes=-1; expect_chunks $BIGSZ $CS 19 19) | sort)")" ok

	fetch "/static/10m.bin?r=$RUN-s5" -H "Range: bytes=20000000-20000100"
	check "超出文件大小的 Range: 416" "$CODE" 416

	section "分片 512KB: 边界文件"
	local SD=$DIR/static/shard f
	# 文件名:期望的回源 Range. 回源层收到边缘层的每个分片请求时都先按整片探测, 所以尾片也是整片区间, 由源站截断
	for f in "exact-1m.bin:bytes=0-524287 bytes=524288-1048575" \
		"plus1.bin:bytes=0-524287 bytes=524288-1048575" \
		"small-100k.bin:bytes=0-524287"; do
		local name=${f%%:*} expect
		expect=$(echo "${f#*:}" | tr ' ' '\n' | sort)
		U="/static/shard/$name?r=$RUN"
		mark
		fetch "$U"
		check "$name($(stat -c %s $SD/$name) 字节): 200 内容一致" "$CODE $(body_md5)" "200 $(file_md5 $SD/$name)"
		check "$name: 回源分片" "$(same "$(ranges "GET $U ")" "$expect")" ok
	done
	fetch "/static/shard/empty.bin?r=$RUN"
	check "空文件: 200, Content-Length 0" "$CODE $(hdr Content-Length)" "200 0"

	section "分片 512KB: 异常与其它"
	U="/norange/10m.bin?r=$RUN"
	mark
	fetch "$U"
	check "源站不支持 Range: 降级为整文件回源, 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
	check "源站不支持 Range: 回源不超过 2 次(1 次探测 + 1 次整文件)" "$(hits "GET $U ")" "[12]"
	fetch "$U"
	check "源站不支持 Range: 再次请求 HIT 内容一致" "$(hdr X-Cache-Status) $(body_md5)" "HIT $BIGMD5"

	fetch "/static/not-exist.bin?r=$RUN"
	check "源站 404: 透传 404" "$CODE" 404

	mark
	fetch "/static/10m.bin?r=$RUN-s6" -I
	check "HEAD: 200, Content-Length 为整文件大小" "$CODE $(hdr Content-Length)" "200 $BIGSZ"
	info "HEAD 未缓存的文件: 源站收到 $(hits "GET /static/10m.bin?r=$RUN-s6 ") 次 GET 分片请求"

	section "分片 512KB: 不缓存的文件不分片"
	# 回源次数: 回源层探测 + 回源层回源(边缘层的探测) + 回源层探测 + 回源层回源(边缘层放弃分片后的请求)
	for U in "/nc/10m.bin?r=$RUN" "/nc-norange/10m.bin?r=$RUN"; do
		mark
		fetch "$U"
		check "$U: 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
		check "$U: 回源不超过 4 次(不按 20 片回源)" "$(hits "GET $U ")" "[1-4]"
	done
	U="/nc/10m.bin?r=$RUN-range"
	mark
	range_check "不缓存的文件 Range" "$U" "5000000-5000999" 5000000 1000
	check "不缓存的文件 Range: 回源不超过 4 次" "$(hits "GET $U ")" "[1-4]"

	section "分片 512KB: 已缓存部分分片后源站换了同样大小的新版本"
	local V=$DIR/static/shard/ver.bin VA=$DIR/static/shard/flip-a.bin VB=$DIR/static/shard/flip-b.bin
	U="/static/shard/ver.bin?r=$RUN"
	cp $VA $V
	touch -d "@$(($(date +%s) - 100))" $V # 保证两个版本的 ETag / Last-Modified 不同
	range_check "版本 1: 缓存第 2 片" "$U" "600000-600099" 600000 100 $VA
	cp $VB $V
	fetch "$U"
	check "版本 2: 请求整文件, 全部是新版本内容(不拼接)" "$CODE $(body_md5)" "200 $(file_md5 $VB)"
	fetch "$U"
	check "版本 2: 再次请求 HIT, 内容一致" "$(hdr X-Cache-Status) $(body_md5)" "HIT $(file_md5 $VB)"

	section "修改分片大小"
	shard_to 1MB "bytes=0-1048575"
	U="/static/10m.bin?r=$RUN-s7"
	mark
	fetch "$U"
	check "新文件: 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
	check "新文件: 按 1MB 分 10 片回源" "$(same "$(ranges "GET $U ")" "$(expect_chunks $BIGSZ 1048576)")" ok
	U="/static/10m.bin?r=$RUN-s1"
	mark
	fetch "$U"
	check "512KB 时缓存的旧文件: HIT 内容一致" "$(hdr X-Cache-Status) $(body_md5)" "HIT $BIGMD5"
	range_check "512KB 时缓存的旧文件: Range 跨分片边界" "$U" "524000-525000" 524000 1001
	check "512KB 时缓存的旧文件: 不回源" "$(hits "GET $U ")" 0

	shard_to 0 -
	mark
	fetch "$U"
	check "关闭分片后, 分片缓存的旧文件: HIT 内容一致" "$(hdr X-Cache-Status) $(body_md5)" "HIT $BIGMD5"
	range_check "关闭分片后, 分片缓存的旧文件: Range" "$U" "5000000-5000999" 5000000 1000
	check "关闭分片后, 分片缓存的旧文件: 不回源" "$(hits "GET $U ")" 0
	U="/static/10m.bin?r=$RUN-s8"
	mark
	fetch "$U"
	check "关闭分片后, 新文件: 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
	check "关闭分片后, 新文件: 整文件回源 1 次, 不带 Range" "$(hits "GET $U ") range=$(ranges "GET $U ")" "1 range=-"
}

# ===========================================================================
test_shard302() {
	section "分片 + 302 跟随关闭"
	shard_to 512KB "bytes=0-524287"
	follow_to 0
	fetch "/302/big?r=$RUN-off"
	check "302 原样返回" "$CODE $(hdr Location)" "302 /static/10m.bin"

	section "分片 512KB + 302 跟随(acc_follow302_max=3)"
	follow_to 3

	local U="/302/big?r=$RUN"
	mark
	fetch "$U"
	check "跳到 10MB 文件: 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
	check "每个分片都先请求 302 地址(20 次)" "$(hits "GET $U ")" 20
	check "跳转后按 20 片回源, 区间正确且不重复" "$(same "$(ranges 'GET /static/10m.bin HTTP')" "$(expect_chunks $BIGSZ $CS)")" ok
	mark
	fetch "$U"
	check "再次请求: HIT 内容一致, 不回源" "$(hdr X-Cache-Status) $(body_md5) $(hits "GET $U ")" "HIT $BIGMD5 0"
	range_check "已缓存 Range" "$U" "5000000-5000999" 5000000 1000

	U="/302/big?r=$RUN-range"
	mark
	range_check "未缓存 Range(第 10 片内)" "$U" "5000000-5000999" 5000000 1000
	check "只回源第 10 片" "$(same "$(ranges 'GET /static/10m.bin HTTP')" "$(expect_chunks $BIGSZ $CS 9 9)")" ok

	U="/302/big?r=$RUN-cross"
	mark
	range_check "未缓存 Range(跨第 1、2 片)" "$U" "524000-525000" 524000 1001
	check "只回源第 1、2 片" "$(same "$(ranges 'GET /static/10m.bin HTTP')" "$(expect_chunks $BIGSZ $CS 0 1)")" ok

	U="/302/big?r=$RUN-suffix"
	range_check "未缓存 Range 末尾(-1000)" "$U" "-1000" $((BIGSZ - 1000)) 1000

	U="/302/big-chain/3?r=$RUN"
	mark
	fetch "$U"
	check "连跳 3 次到大文件: 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
	check "每个分片都重新跟随 3 跳(20x3 次)" "$(hits 'GET /302/big-chain/')" 60
	fetch "/302/big-chain/4?r=$RUN"
	check "连跳 4 次(超上限): 返回最后一跳的 302" "$CODE $(hdr Location)" "302 /static/10m.bin"

	fetch "/302/big-abs?r=$RUN"
	check "绝对地址(本域名)跳到大文件: 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"

	U="/302/norange?r=$RUN"
	mark
	fetch "$U"
	check "跳到不支持 Range 的地址: 降级为整文件, 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
	check "跳到不支持 Range 的地址: 回源不超过 2 次" "$(hits 'GET /norange/10m.bin')" "[12]"

	fetch "/302/big-404?r=$RUN"
	check "跳转目标 404: 返回 404" "$CODE" 404

	fetch "/302/big?r=$RUN-head" -I
	check "HEAD: 200, Content-Length 为整文件大小" "$CODE $(hdr Content-Length)" "200 $BIGSZ"

	section "分片 + 302 跳到外网第三方"
	local EXT=$T/ext.deb
	if curl -s -m 60 -o $EXT http://mirrors.aliyun.com/ubuntu/pool/main/v/vim/vim-runtime_9.1.0016-1ubuntu7_all.deb; then
		fetch "/302/third-big?r=$RUN" -H "Cookie: sid=abc"
		check "跳到外网大文件($(stat -c %s $EXT) 字节, 14 片): 200 内容一致" "$CODE $(body_md5)" "200 $(file_md5 $EXT)"
		range_check "跳到外网大文件: 未缓存 Range(跨分片)" "/302/third-big?r=$RUN-range" "3000000-3200000" 3000000 200001 $EXT
	else
		check "下载外网对照文件" "失败" "成功"
	fi
	fetch "/302/third-range?r=$RUN"
	check "跳到 httpbin 小文件(100KB, 小于 1 片): 整文件请求 200" "$CODE" 200
	check "跳到 httpbin 小文件: 内容正确" "$(pattern_ok 0 102400)" ok

	section "分片 + 302: 分片之间的一致性(已知限制)"
	# 同一次拉取中不逐片比对 ETag(302 指向内容相同的多台镜像时 ETag 常常不同, 严格比对会误判),
	# 两个文件大小相同时无法发现, 这里只记录现象
	local A=$DIR/static/shard/flip-a.bin B=$DIR/static/shard/flip-b.bin r
	fetch "/302/flip?r=$RUN"
	r=$(body_md5)
	if [ "$r" = "$(file_md5 $A)" ] || [ "$r" = "$(file_md5 $B)" ]; then
		r="完整的单个文件"
	elif [ "$(head -c $CS $T/body | md5sum | cut -d' ' -f1)" = "$(slice_md5 $A 0 $CS)" ] &&
		[ "$(tail -c +$((CS + 1)) $T/body | md5sum | cut -d' ' -f1)" = "$(slice_md5 $B $CS $CS)" ]; then
		r="前 512KB 来自 flip-a, 后 512KB 来自 flip-b"
	fi
	info "302 目标在两个大小相同的文件间交替: $CODE $r"

	# 恢复本组改过的配置, 后面的组从 302 跟随和分片都关闭的状态开始
	section "分片 + 302: 恢复"
	follow_to 0
	shard_to 0 -
}

# ===========================================================================
test_share() {
	section "共享缓存域名: 两个域名命中同一份缓存"
	share_to $HOST on

	local U="/dyn/share?r=$RUN" b1 line
	mark
	fetch "$U"
	b1=$(body)
	on_host $SHARE_HOST fetch "$U"
	check "test 域名回源后, share 域名 HIT" "$(hdr X-Cache-Status)" HIT
	check "share 域名拿到同一份内容" "$(body)" "$b1"
	check "源站只被请求 1 次" "$(hits "GET $U ")" 1

	U="/dyn/share-rev?r=$RUN"
	mark
	on_host $SHARE_HOST fetch "$U"
	b1=$(body)
	line=$(since_mark | grep -F "GET $U " | head -1)
	check "share 域名先请求: 按 share 自己的配置回源(Host 为 $SHARE_HOST)" "$(echo "$line" | awk '{print $4}')" "\"$SHARE_HOST\""
	fetch "$U"
	check "之后 test 域名 HIT, 内容相同" "$(hdr X-Cache-Status) $(body)" "HIT $b1"
	check "源站只被请求 1 次" "$(hits "GET $U ")" 1

	U="/static/10m.bin?r=$RUN-share"
	mark
	fetch "$U"
	on_host $SHARE_HOST range_check "test 域名缓存的大文件, share 域名 Range" "$U" "5000000-5000999" 5000000 1000
	check "share 域名 Range 不回源" "$(hits "GET $U ")" 1

	section "共享缓存域名: URL 刷新"
	local F=share-url-$RUN.txt
	echo "v1-$RUN" >$DIR/static/$F
	fetch "/static/$F"
	on_host $SHARE_HOST fetch "/static/$F"
	check "刷新前: share 域名命中 test 域名缓存的 v1" "$(hdr X-Cache-Status) $(body)" "HIT v1-$RUN"
	echo "v2-$RUN" >$DIR/static/$F
	local id
	id=$(submit_task refresh "{\"type\":\"file\",\"data_list\":[\"http://$SHARE_HOST/static/$F\"]}")
	check "用 share 域名的 URL 提交刷新" "$id" "$UUID_GLOB"
	wait_until "URL 刷新生效" body_is "/static/$F" "v2-$RUN"
	check "test 域名拿到 v2" "$(body)" "v2-$RUN"
	on_host $SHARE_HOST fetch "/static/$F"
	check "share 域名也是 v2" "$(body)" "v2-$RUN"
	task_done refresh "$id"
	check "刷新任务 Success, dispatcher 先刷回源层再刷边缘层" "$(task_status refresh "$id") $(refresh_order "$id")" "Success $ORDER_OK"

	F=share-url2-$RUN.txt
	echo "v1-$RUN" >$DIR/static/$F
	on_host $SHARE_HOST fetch "/static/$F"
	fetch "/static/$F"
	check "刷新前: test 域名命中 share 域名缓存的 v1" "$(hdr X-Cache-Status) $(body)" "HIT v1-$RUN"
	echo "v2-$RUN" >$DIR/static/$F
	id=$(submit_task refresh "{\"type\":\"file\",\"data_list\":[\"http://$HOST/static/$F\"]}")
	check "用 test 域名的 URL 提交刷新" "$id" "$UUID_GLOB"
	wait_until "URL 刷新生效" on_host $SHARE_HOST body_is "/static/$F" "v2-$RUN"
	check "share 域名拿到 v2" "$(body)" "v2-$RUN"
	task_done refresh "$id"
	check "刷新任务 Success, dispatcher 先刷回源层再刷边缘层" "$(task_status refresh "$id") $(refresh_order "$id")" "Success $ORDER_OK"

	section "共享缓存域名: 目录刷新"
	local D=share-dir-$RUN
	mkdir -p $DIR/static/$D
	echo "v1-$RUN" >$DIR/static/$D/a.txt
	fetch "/static/$D/a.txt"
	on_host $SHARE_HOST fetch "/static/$D/a.txt"
	check "刷新前: share 域名命中 test 域名缓存的 v1" "$(hdr X-Cache-Status) $(body)" "HIT v1-$RUN"
	echo "v2-$RUN" >$DIR/static/$D/a.txt
	id=$(submit_task refresh "{\"type\":\"directory\",\"data_list\":[\"http://$SHARE_HOST/static/$D/\"]}")
	task_done refresh "$id" 3
	check "用 share 域名提交目录刷新, 任务 Success" "$(task_status refresh "$id")" Success
	wait_until "share 域名目录刷新生效" on_host $SHARE_HOST body_is "/static/$D/a.txt" "v2-$RUN"
	check "share 域名拿到 v2" "$(body)" "v2-$RUN"
	WAIT_TRIES=10 wait_until "test 域名目录刷新生效" body_is "/static/$D/a.txt" "v2-$RUN"
	check "test 域名也拿到 v2" "$(body)" "v2-$RUN"

	D=share-dir2-$RUN
	mkdir -p $DIR/static/$D
	echo "v1-$RUN" >$DIR/static/$D/a.txt
	fetch "/static/$D/a.txt"
	on_host $SHARE_HOST fetch "/static/$D/a.txt"
	check "刷新前: share 域名命中 test 域名缓存的 v1" "$(hdr X-Cache-Status) $(body)" "HIT v1-$RUN"
	echo "v2-$RUN" >$DIR/static/$D/a.txt
	id=$(submit_task refresh "{\"type\":\"directory\",\"data_list\":[\"http://$HOST/static/$D/\"]}")
	task_done refresh "$id" 3
	check "用 test 域名提交目录刷新, 任务 Success" "$(task_status refresh "$id")" Success
	wait_until "test 域名目录刷新生效" body_is "/static/$D/a.txt" "v2-$RUN"
	check "test 域名拿到 v2" "$(body)" "v2-$RUN"
	WAIT_TRIES=10 wait_until "share 域名目录刷新生效" on_host $SHARE_HOST body_is "/static/$D/a.txt" "v2-$RUN"
	check "share 域名也拿到 v2" "$(body)" "v2-$RUN"

	section "共享缓存域名: 取消共享后各自缓存(对照)"
	share_to "" off
	U="/dyn/share-off?r=$RUN"
	mark
	fetch "$U"
	on_host $SHARE_HOST fetch "$U"
	check "取消共享后 share 域名 MISS" "$(hdr X-Cache-Status)" MISS
	check "源站被请求 2 次" "$(hits "GET $U ")" 2
	share_to $HOST on
}

# ===========================================================================
echo "AresCDN 核心功能测试 run=$RUN edge=$EDGE host=$HOST groups=[$TEST_GROUPS]"

section "准备: 关闭 302 跟随和分片"
follow_to 0
shard_to 0 -

for g in $TEST_GROUPS; do
	case $g in
	basic) test_basic ;;
	follow) test_follow ;;
	shard) test_shard ;;
	shard302) test_shard302 ;;
	share) test_share ;;
	*) echo "未知的组: $g" && exit 2 ;;
	esac
done

section "恢复: 关闭 302 跟随和分片"
follow_to 0
shard_to 0 -

echo
echo -e "${B}结果: 通过 $PASS, 失败 $FAIL${N}"
for f in "${FAILED[@]}"; do echo -e "  ${R}FAIL${N} $f"; done
[ $FAIL -eq 0 ]
