#!/bin/bash
# AresCDN 核心功能测试
#
# 在控制面机器上执行(先执行过 setup.sh): sudo ./run-tests.sh [组...]
#   basic    回源链路 / 缓存规则 / 大文件与 Range / 刷新 / 预热
#   follow   301 / 302 跟随
#   shard    分片
#   shard302 分片 + 301 / 302 跟随
#   share    共享缓存域名(SHARE_HOST 共享 HOST 的缓存)
#   errcode  错误码缓存
#   fault    源站故障: 超时、连上就断开、响应到一半断开、连不上
#   stats    统计: edge -> cache-manager -> Kafka -> metric-consumer -> ClickHouse -> api 统计接口
#   不带参数时执行全部.
#
# 链路: 本机 curl -> 边缘层节点(EDGE) -> 回源层节点(ORIGIN_LAYER_IP) -> 本机测试源站 :8081
# 环境配置见 lab.env; 测试域名组的配置通过 tools/cdnapi.sh 调用 API 修改,
# 每组结束时恢复本组改过的配置, 退出时跟随、分片关闭, 共享缓存域名恢复为 HOST, 错误码缓存规则清空,
# 回源配置恢复为 fault 组改之前的.
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
TEST_GROUPS=${*:-basic follow shard shard302 share errcode fault stats}
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
# (取自 dispatcher 的 debug 日志 "Refresh done: 请求, 域名, 任务, 层级, 节点, 状态";
#  机器非正常关机后日志里可能有 NUL 字节, 所以 grep -a)
refresh_order() {
	grep -a -h "Refresh done: $1" $(ls -t "$DISPATCHER_LOG_DIR"/* | head -1) |
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
		wait_until "跟随关闭生效" probe_code /302/rel 302
	else
		wait_until "跟随开启生效" probe_code /302/rel 200
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

set_errcode() { api PATCH /api/cdn/v1/domaingroups/errorpage_cache "{\"unique_id\":\"$DG\",\"rule_list\":$1}"; }
# 规则是否生效: 新地址的 404 请求两次, 第二次 HIT 表示缓存 404(on), MISS 表示不缓存(off)
errcode_is() {
	local p="/err/404/probe-$RUN-$RANDOM"
	fetch "$p"
	fetch "$p"
	if [ "$1" = on ]; then
		[ "$(hdr X-Cache-Status)" = HIT ]
	else
		[ "$(hdr X-Cache-Status)" = MISS ]
	fi
}
errcode_to() { # errcode_to 规则列表(JSON) on|off(404 是否缓存)
	local desc="设置错误码缓存规则"
	[ "$1" = "[]" ] && desc="清空错误码缓存规则"
	check "API $desc" "$(set_errcode "$1")" ok
	wait_until "$desc 生效" errcode_is "$2"
	sleep $CONFIG_SETTLE_SECS
}
# twice 路径 [curl 参数...]: 请求两次, 输出第二次的缓存状态, 以及和第一次是不是同一个源站响应(X-Origin-Seq)
twice() {
	local seq
	fetch "$@"
	seq=$(hdr X-Origin-Seq)
	fetch "$@"
	if [ "$(hdr X-Origin-Seq)" = "$seq" ]; then
		echo "$(hdr X-Cache-Status) 同一响应"
	else
		echo "$(hdr X-Cache-Status) 重新回源"
	fi
}
# again 路径 序号: 再请求一次, 输出缓存状态, 以及是不是仍是这个序号的源站响应
again() {
	fetch "$1"
	if [ "$(hdr X-Origin-Seq)" = "$2" ]; then
		echo "$(hdr X-Cache-Status) 同一响应"
	else
		echo "$(hdr X-Cache-Status) 重新回源"
	fi
}
# 只清边缘层节点的缓存: edge 允许内网地址直接发 PURGE, 只作用于本节点, 不经过 dispatcher
purge_edge() { curl -s -o /dev/null -w '%{http_code}' -X PURGE -H "Host: $HOST" "http://$EDGE$1"; }

# fetch_timed PATH [curl 参数...]: 同 fetch, 另外记下耗时 $SECS(秒)和 curl 的退出码 $RC
fetch_timed() {
	local path=$1
	shift
	: >$T/hdr
	: >$T/body
	SECS=$(curl -s -m 60 -o $T/body -D $T/hdr -w '%{time_total}' -H "Host: $HOST" "$@" "http://$EDGE$path")
	RC=$?
	CODE=$(awk 'NR==1{print $2}' $T/hdr)
}
# between 值 下限 上限: 在范围内输出 ok, 否则输出这个值
between() { awk -v v="$1" -v lo="$2" -v hi="$3" 'BEGIN{if (v >= lo && v <= hi) print "ok"; else print v}'; }

# 回源配置(JSON). fault 组改之前存在 ORIGIN_SAVED, 退出时恢复
ORIGIN_SAVED=
origin_json() { $API GET /api/cdn/v1/domaingroups | jq -c --arg id "$DG" '.data.domain_group_list[] | select(.unique_id == $id) | .origin'; }
set_origin() { api PATCH /api/cdn/v1/domaingroups "{\"unique_id\":\"$DG\",\"origin\":$1}"; }

cleanup() {
	set_follow 0 >/dev/null
	set_shard 0 >/dev/null
	set_as_domain $HOST >/dev/null
	set_errcode '[]' >/dev/null
	[ -n "$ORIGIN_SAVED" ] && set_origin "$ORIGIN_SAVED" >/dev/null
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
	section "跟随关闭时"
	follow_to 0
	fetch "/302/rel?r=$RUN-off"
	check "302 原样返回" "$CODE $(hdr Location)" "302 /dyn/rel-final"
	fetch "/301/rel?r=$RUN-off"
	check "301 原样返回" "$CODE $(hdr Location)" "301 /dyn/301-final"

	section "301 / 302 跟随(acc_follow302_max=3)"
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
	check "301 相对地址: 200, 返回跳转后的内容" "$CODE $(body)" "200 dyn uri=/dyn/301-final *"
	fetch "/mix/chain/3?r=$RUN"
	check "301 / 302 混合连跳 3 次(等于上限): 200" "$CODE $(body)" "200 dyn uri=/dyn/mix-final *"
	mark
	fetch "/mix/chain/4?r=$RUN"
	check "混合连跳 4 次(超上限): 返回最后一跳的 301" "$CODE $(hdr Location)" "301 /dyn/mix-final"
	check "混合连跳 4 次: 源站收到 4 次请求" "$(hits 'GET /mix/chain/')" 4
	fetch "/301/none?r=$RUN"
	check "301 没有 Location: 返回 301" "$CODE" 301
	check "301 没有 Location: 响应也没有 Location" "$(hdr Location)" ""
	mark
	fetch "/301/third-private?r=$RUN"
	check "301 跳到内网地址: 不跟随, 返回 301" "$CODE $(hdr Location)" "301 http://$ORIGIN_IP:8082/dyn/third301"
	check "301 跳到内网地址: 没有访问内网" "$(hits ' 8082 "')" 0

	fetch "/307/rel?r=$RUN"
	check "307 不跟随" "$CODE $(hdr Location)" "307 /dyn/307-final"
	fetch "/308/rel?r=$RUN"
	check "308 不跟随" "$CODE $(hdr Location)" "308 /dyn/308-final"
	fetch "/302/rel?r=$RUN-post" -X POST -d a=1
	check "POST 不跟随" "$CODE" 302
	fetch "/301/rel?r=$RUN-post" -X POST -d a=1
	check "POST 不跟随 301" "$CODE" 301
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
	section "分片 + 跟随关闭"
	shard_to 512KB "bytes=0-524287"
	follow_to 0
	fetch "/302/big?r=$RUN-off"
	check "302 原样返回" "$CODE $(hdr Location)" "302 /static/10m.bin"

	section "分片 512KB + 301 / 302 跟随(acc_follow302_max=3)"
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

	U="/302/big-301?r=$RUN"
	mark
	fetch "$U"
	check "301 跳到 10MB 文件: 200 内容一致" "$CODE $(body_md5)" "200 $BIGMD5"
	check "301: 每个分片都先请求 301 地址(20 次)" "$(hits "GET $U ")" 20
	check "301: 跳转后按 20 片回源, 区间正确且不重复" "$(same "$(ranges 'GET /static/10m.bin HTTP')" "$(expect_chunks $BIGSZ $CS)")" ok
	mark
	fetch "$U"
	check "301: 再次请求 HIT 内容一致, 不回源" "$(hdr X-Cache-Status) $(body_md5) $(hits "GET $U ")" "HIT $BIGMD5 0"

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

	# 恢复本组改过的配置, 后面的组从跟随和分片都关闭的状态开始
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
# 错误码缓存, 用测试源站的 /err/<状态码>. 规则按优先级从高到低:
#   目录 /err/403/in/、/err/429/prio/: 403=60,429=60
#   后缀 html: 405=60
#   全部: 404=60,410=20,429=2,503=60
ERRCODE_RULES='[
	{"rule_type":"directory","rule_path_list":["/err/403/in/","/err/429/prio/"],"content":"403=60,429=60","priority":10},
	{"rule_type":"file","rule_path_list":["html"],"content":"405=60","priority":5},
	{"rule_type":"all","rule_path_list":["*"],"content":"404=60,410=20,429=2,503=60","priority":1}
]'

test_errcode() {
	section "错误码缓存: 设置规则"
	errcode_to "$ERRCODE_RULES" on
	local p s ttl prio maxage id cc

	section "错误码缓存: 基本"
	mark
	check "404: 第二次命中缓存" "$(twice /err/404/a-$RUN)" "HIT 同一响应"
	check "404: 只回源 1 次" "$(hits "GET /err/404/a-$RUN ")" 1
	check "没配的 502: 不缓存" "$(twice /err/502/a-$RUN)" "MISS 重新回源"
	fetch /err/502/once-$RUN
	check "没缓存的 5xx: 源站只收到 1 次(edge 不在 5xx 时换 cache 设备重试)" "$(hits "GET /err/502/once-$RUN ")" 1
	fetch /err/404/head-$RUN -I
	check "HEAD 的 404 也缓存(cache 把 HEAD 当 GET), 之后 GET 命中" "$(again /err/404/head-$RUN "$(hdr X-Origin-Seq)")" "HIT 同一响应"
	check "POST 的 404 不缓存" "$(twice /err/404/post-$RUN -d x)" "MISS 重新回源"

	section "错误码缓存: 规则匹配、过期、优先级"
	check "目录规则: /err/403/in/ 下的 403 缓存" "$(twice /err/403/in/a-$RUN)" "HIT 同一响应"
	check "目录规则: 其它目录的 403 不缓存" "$(twice /err/403/out/a-$RUN)" "MISS 重新回源"
	check "后缀规则: .html 的 405 缓存" "$(twice /err/405/a-$RUN.html)" "HIT 同一响应"
	check "后缀规则: .txt 的 405 不缓存" "$(twice /err/405/a-$RUN.txt)" "MISS 重新回源"
	fetch /err/429/ttl-$RUN
	ttl=$(hdr X-Origin-Seq)
	fetch /err/429/prio/a-$RUN
	prio=$(hdr X-Origin-Seq)
	fetch "/err/404/maxage-$RUN?cc=max-age%3D1"
	maxage=$(hdr X-Origin-Seq)
	check "429=2: 2 秒内命中" "$(again /err/429/ttl-$RUN $ttl)" "HIT 同一响应"
	sleep 3
	check "429=2: 过期后重新回源" "$(again /err/429/ttl-$RUN $ttl)" "MISS 重新回源"
	check "优先级: 目录规则的 429=60 优先于全部规则的 429=2, 3 秒后仍命中" "$(again /err/429/prio/a-$RUN $prio)" "HIT 同一响应"
	check "缓存时间以规则为准: 源站 max-age=1, 3 秒后仍命中" "$(again "/err/404/maxage-$RUN?cc=max-age%3D1" $maxage)" "HIT 同一响应"

	section "错误码缓存: 源站响应头"
	check "404 带 Set-Cookie: 不缓存" "$(twice "/err/404/cookie-$RUN?cookie=1")" "MISS 重新回源"
	check "404 带 Set-Cookie: 拿到的是本次响应的 cookie" "$(hdr Set-Cookie)" "sid=$(hdr X-Origin-Seq); Path=/"
	check "503 带 Set-Cookie: 不缓存" "$(twice "/err/503/cookie-$RUN?cookie=1")" "MISS 重新回源"
	for cc in no-store no-cache private; do
		check "404 带 Cache-Control: $cc: 不缓存" "$(twice "/err/404/cc-$cc-$RUN?cc=$cc")" "MISS 重新回源"
	done
	check "503 带 no-store, no-cache, private: 仍缓存(5xx 忽略这三个指令)" \
		"$(twice "/err/503/cc-$RUN?cc=no-store,no-cache,private")" "HIT 同一响应"

	section "错误码缓存: 回源层"
	p=/err/404/layer-$RUN
	mark
	fetch $p
	s=$(hdr X-Origin-Seq)
	fetch $p
	check "只清 $EDGE_NODE 的缓存" "$(purge_edge $p)" 204
	check "再请求: $EDGE_NODE 回源, $ORIGIN_NODE 命中(同一个源站响应)" "$(again $p $s)" "MISS 同一响应"
	check "源站只收到 1 次请求" "$(hits "GET $p ")" 1
	p=/err/410/age-$RUN
	fetch $p
	s=$(hdr X-Origin-Seq)
	sleep 12
	check "410=20: 12 秒后只清 $EDGE_NODE 的缓存" "$(purge_edge $p)" 204
	fetch $p
	check "从 $ORIGIN_NODE 拿到的 410 带 Age(约 12 秒)" "$(hdr X-Cache-Status) $(hdr Age)" "MISS 1[123]"
	sleep 12
	check "$EDGE_NODE 扣掉 Age: 第 24 秒两层都已过期, 重新回源" "$(again $p $s)" "MISS 重新回源"

	section "错误码缓存: URL 刷新"
	p=/err/404/refresh-$RUN
	fetch $p
	s=$(hdr X-Origin-Seq)
	check "刷新前命中" "$(again $p $s)" "HIT 同一响应"
	id=$(submit_task refresh "{\"type\":\"file\",\"data_list\":[\"http://$HOST$p\"]}")
	check "提交 URL 刷新" "$id" "$UUID_GLOB"
	task_done refresh "$id"
	check "刷新任务 Success, dispatcher 先刷回源层再刷边缘层" "$(task_status refresh "$id") $(refresh_order "$id")" "Success $ORDER_OK"
	check "URL 刷新后重新回源" "$(again $p $s)" "MISS 重新回源"

	section "错误码缓存: 恢复"
	errcode_to '[]' off
}

# ===========================================================================
# 源站故障, 用测试源站的 /slow/<秒>、/reset、/truncate; 连不上时临时把回源端口改成 DEAD_PORT.
# 检查 CDN 每一层都不重试(源站只收到 1 次)、残缺的响应不缓存、源站超时的 504 可以用错误码缓存挡住
DEAD_PORT=8089

test_fault() {
	local rt slow p
	rt=$(origin_json | jq -r .timeouts.read_timeout)
	slow=$((rt + 1))

	section "源站故障: 超时(回源读超时 $rt 秒, 源站 $slow 秒才响应)"
	mark
	p=/slow/$slow/a-$RUN
	fetch_timed $p
	check "返回 504" "$CODE" 504
	check "约 $rt 秒返回: 超时后直接返回, 没有换设备再等一轮" "$(between $SECS $((rt - 1)) $((rt + 2)))" ok
	sleep 2 # 源站 $slow 秒后才写访问日志
	check "源站只收到 1 次" "$(hits "GET $p ")" 1

	errcode_to "[
		{\"rule_type\":\"directory\",\"rule_path_list\":[\"/slow/$slow/c/\"],\"content\":\"504=60\",\"priority\":2},
		{\"rule_type\":\"all\",\"rule_path_list\":[\"*\"],\"content\":\"404=60\",\"priority\":1}]" on
	p=/slow/$slow/c/a-$RUN
	fetch_timed $p
	check "配了 504=60: 第一次等到超时, 返回 504" "$CODE $(between $SECS $((rt - 1)) $((rt + 2)))" "504 ok"
	fetch_timed $p
	check "配了 504=60: 第二次直接返回缓存的 504" "$CODE $(hdr X-Cache-Status) $(between $SECS 0 1)" "504 HIT ok"
	sleep 2
	check "源站只收到 1 次" "$(hits "GET $p ")" 1
	errcode_to '[]' off

	section "源站故障: 连上就断开"
	p=/reset/a-$RUN
	fetch $p
	check "返回 502" "$CODE" 502
	sleep 1
	# 回源开了 keepalive: 复用的空闲连接没收到响应头就被断开时, nginx 当成连接过期, 在新连接上再试一次
	check "源站最多收到 2 次(只有回源这一跳在复用连接被断开时再试一次)" "$(hits "GET $p ")" "[12]"

	section "源站故障: 响应到一半断开(声明 1000 字节, 只发 100 字节, 带 max-age=3600)"
	p=/truncate/a-$RUN
	fetch_timed $p
	check "客户端收到 100 字节后连接中断(curl 退出码 18)" "$CODE $(wc -c <$T/body) $RC" "200 100 18"
	fetch_timed $p
	check "残缺的响应没有被缓存: 第二次仍是 MISS" "$(hdr X-Cache-Status)" MISS
	sleep 1
	check "源站收到 2 次" "$(hits "GET $p ")" 2

	section "源站故障: 连不上(回源端口改成没有监听的 $DEAD_PORT)"
	check "本机 $DEAD_PORT 端口没有监听" "$(ss -ltnH "sport = :$DEAD_PORT" | wc -l)" 0
	ORIGIN_SAVED=$(origin_json)
	check "API 把回源端口改成 $DEAD_PORT" "$(set_origin "$(echo "$ORIGIN_SAVED" | jq -c ".origin_info_list[0].port = $DEAD_PORT")")" ok
	wait_until "回源端口修改生效" probe_code /dyn/fault 502
	sleep $CONFIG_SETTLE_SECS
	fetch_timed /dyn/dead-$RUN
	check "很快返回 502: 不等超时、不重试" "$CODE $(between $SECS 0 2)" "502 ok"
	check "API 恢复回源配置" "$(set_origin "$ORIGIN_SAVED")" ok
	wait_until "回源恢复" probe_code /dyn/fault 200
	sleep $CONFIG_SETTLE_SECS
	ORIGIN_SAVED=
}

# ===========================================================================
# 统计: edge 每个 worker 按分钟聚合, 每 10 秒上报本机 cache-manager -> Kafka -> metric-consumer -> ClickHouse t_cdn_metrics.
# 本组从新的一分钟开始发请求, 窗口内只有本组的请求(期间不能有其它程序访问 HOST / SHARE_HOST), 所以可以精确比对.
CK_CONTAINER=${CLICKHOUSE_CONTAINER:-arescdn-clickhouse}
STAT_TZ=${STAT_TZ:-Asia/Shanghai} # api 统计接口的时区(系统设置 arescdn_time_zone, 默认 Asia/Shanghai)

# ck: 在 ClickHouse 容器里执行 stdin 中的查询, 输出 TSV
ck() { docker exec -i "$CK_CONTAINER" sh -c 'clickhouse-client --password "$CLICKHOUSE_PASSWORD" -d arescdn --format TSV'; }

# sreq 域名 方法 路径 [curl 参数...]: 发请求, 把实际结果追加到 $T/stats.tsv:
#   域名 方法 状态码 X-Cache-Status 是否中断 字节数(响应头+响应体) 耗时(毫秒)
# --raw: 不解分块编码, 字节数包含分块的长度行, 与服务端的 bytes_sent 一致
sreq() {
	local host=$1 method=$2 path=$3 w abort=0
	shift 3
	case $method in
	GET) ;;
	HEAD) set -- -I "$@" ;;
	*) set -- -X "$method" "$@" ;;
	esac
	: >$T/hdr
	w=$(curl -s --raw -m 60 -o /dev/null -D $T/hdr -H "Host: $host" \
		-w '%{http_code} %{size_header} %{size_download} %{time_total}' "$@" "http://$EDGE$path")
	[ $? = 28 ] && abort=1 # 超时: 客户端主动断开
	set -- $w
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$host" "$method" "$1" "$(hdr X-Cache-Status)" $abort \
		$(($2 + $3)) "$(awk -v t="$4" 'BEGIN{printf "%d", t * 1000 + 0.5}')" >>$T/stats.tsv
}
# stats_expect: 实际的请求数, 按 域名 方法 状态码 缓存状态 是否中断 分组
stats_expect() { awk -F'\t' -v OFS='\t' '{n[$1 OFS $2 OFS $3 OFS $4 OFS $5]++} END{for (k in n) print k, n[k]}' $T/stats.tsv | LC_ALL=C sort; }
# stats_n [域名]: 实际的请求数
stats_n() { awk -F'\t' -v h="${1:-}" 'h == "" || $1 == h' $T/stats.tsv | wc -l; }
# stats_where [域名]: 本组时间窗口内边缘层的记录
stats_where() {
	local d=${1:+"'$1'"}
	echo "layer = 'edge' AND domain IN (${d:-"'$HOST', '$SHARE_HOST'"}) AND time >= toDateTime($ST_S) AND time < toDateTime($ST_E)"
}
stats_ck_ge() { [ "$(ck <<<"SELECT sum(request_count) FROM t_cdn_metrics WHERE $(stats_where)")" -ge "$1" ] 2>/dev/null; }

# stat_api 接口 [查询参数]: 查统计接口, 时间范围为本组的窗口, 输出 data
api_time() { TZ=$STAT_TZ date -d "@$1" '+%Y-%m-%d%%20%H:%M:%S'; }
stat_api() {
	$API GET "/api/cdn/v1/statistics/$1?start_time=$(api_time $ST_S)&end_time=$(api_time $ST_E)&${2:-domain=$HOST&granularity=1min}" | jq -c .data
}
# billing_coef 域名组ID: 计费系数, 没有设置时为 1
billing_coef() { $API GET /api/cdn/v1/domaingroups | jq -r --arg id "$1" '.data.domain_group_list[] | select(.unique_id == $id) | .billing_coef // 1'; }
# near 实际 期望 允许误差: 输出 ok 或差值
near() { awk -v a="$1" -v b="$2" -v e="$3" 'BEGIN{d = a - b; if (d < 0) d = -d; if (d <= e) print "ok"; else printf "实际 %.10g, 期望 %.10g\n", a, b}'; }

test_stats() {
	section "统计: 发请求"
	# 等到新的一分钟再开始, 窗口 [ST_S, ST_E) 按分钟对齐
	sleep $((61 - $(date +%s) % 60))
	ST_S=$(($(date +%s) / 60 * 60))
	: >$T/stats.tsv
	local U="/dyn/st?r=$RUN" id1 id2 F=st-preheat-$RUN.txt
	sreq $HOST GET "$U"
	sreq $HOST GET "$U"
	sreq $HOST GET "$U"
	sreq $SHARE_HOST GET "$U" # 共享缓存: 命中 HOST 的缓存, 记在 SHARE_HOST 下
	sreq $HOST GET "/nocache/st?r=$RUN"
	sreq $HOST GET "/nocache/st?r=$RUN"
	sreq $HOST GET "/static/not-exist-$RUN.bin"
	sreq $HOST GET "/static/10m.bin?r=$RUN-st"
	sreq $HOST GET "/static/10m.bin?r=$RUN-st" -H "Range: bytes=0-99"
	sreq $HOST GET "/static/10m.bin?r=$RUN-st" -H "Range: bytes=20000000-20000100"
	sreq $HOST GET "/302/rel?r=$RUN-st"
	sreq $HOST HEAD "/static/small.txt?r=$RUN-st"
	sreq $HOST POST "/echo?r=$RUN-st" -d a=1
	sreq $HOST GET "/static/10m.bin?r=$RUN-abort" --limit-rate 100k -m 2 # 下载 2 秒后断开
	check "发出的请求: 状态码 / 缓存状态 / 是否中断" \
		"$(cut -f3-5 $T/stats.tsv | tr '\t\n' ' ' | sed 's/ $//')" \
		"200 MISS 0 200 HIT 0 200 HIT 0 200 HIT 0 200 MISS 0 200 MISS 0 404 * 0 200 MISS 0 206 HIT 0 416 * 0 302 * 0 200 * 0 200 * 0 200 MISS 1"

	# 窗口内做一次 URL 刷新和预热, 节点上的 PURGE / PREHEAT 请求不应计入统计
	echo "preheat-$RUN" >$DIR/static/$F
	id1=$(submit_task refresh "{\"type\":\"file\",\"data_list\":[\"http://$HOST$U\"]}")
	id2=$(submit_task preheat "{\"data_list\":[\"http://$HOST/static/$F\"]}")
	task_done refresh "$id1"
	task_done preheat "$id2"
	check "窗口内执行了 URL 刷新和预热" "$(task_status refresh "$id1") $(task_status preheat "$id2")" "Success Success"
	ST_E=$((($(date +%s) / 60 + 1) * 60))

	section "统计: ClickHouse"
	local n nh ns
	n=$(stats_n)
	nh=$(stats_n $HOST)
	ns=$(stats_n $SHARE_HOST)
	WAIT_TRIES=30 wait_until "指标写入 ClickHouse" stats_ck_ge $n
	sleep 12 # 每个 worker 至少再上报一轮: 多算的请求这时也会出现
	check "按 域名/方法/状态码/缓存状态/是否中断 分组的请求数与实际一致($n 个)" \
		"$(same "$(ck <<<"SELECT domain, method, status_code, cache_status, client_abort, sum(request_count)
			FROM t_cdn_metrics WHERE $(stats_where) GROUP BY 1, 2, 3, 4, 5" | LC_ALL=C sort)" "$(stats_expect)")" ok
	check "刷新和预热不计入(窗口内没有 PURGE / PREHEAT)" \
		"$(ck <<<"SELECT count() FROM t_cdn_metrics WHERE method IN ('PURGE', 'PREHEAT') AND time >= toDateTime($ST_S) AND time < toDateTime($ST_E)")" 0
	check "边缘层: 节点 $EDGE_NODE, 设备 cdn$EDGE_NODE-*" \
		"$(ck <<<"SELECT DISTINCT node_name, device_name FROM t_cdn_metrics WHERE $(stats_where)" | tr '\t\n' '  ' | sed 's/ $//')" \
		"$EDGE_NODE cdn$EDGE_NODE-*"
	check "域名组 ID: $HOST 为 $DG, $SHARE_HOST 为 $SHARE_DG" \
		"$(ck <<<"SELECT DISTINCT domain, cdn_id FROM t_cdn_metrics WHERE $(stats_where)" | LC_ALL=C sort | tr '\t\n' '  ' | sed 's/ $//')" \
		"$(printf '%s %s\n' $HOST $DG $SHARE_HOST $SHARE_DG | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//')"
	check "协议 http, IP 版本 ipv4" \
		"$(ck <<<"SELECT DISTINCT protocol, ip_version FROM t_cdn_metrics WHERE $(stats_where)" | tr '\t\n' '  ' | sed 's/ $//')" "http ipv4"
	check "回源层的请求记在 $ORIGIN_NODE, layer=origin(不计入边缘层)" \
		"$(ck <<<"SELECT DISTINCT node_name, layer FROM t_cdn_metrics WHERE layer NOT IN ('edge', 'source') AND domain IN ('$HOST', '$SHARE_HOST')
			AND time >= toDateTime($ST_S) AND time < toDateTime($ST_E)" | tr '\t\n' '  ' | sed 's/ $//')" "$ORIGIN_NODE origin"
	# 回源(最后一层节点向源站发出的请求)由回源端口上报, layer=source
	check "回源记在 $ORIGIN_NODE, layer=source" \
		"$(ck <<<"SELECT DISTINCT node_name, layer FROM t_cdn_metrics WHERE layer = 'source' AND domain IN ('$HOST', '$SHARE_HOST')
			AND time >= toDateTime($ST_S) AND time < toDateTime($ST_E)" | tr '\t\n' '  ' | sed 's/ $//')" "$ORIGIN_NODE source"

	# 字节数和耗时不含中断的请求: 服务端发出的字节数和结束时间与客户端看到的不同.
	# 统计的是计费流量: 每个请求 floor(发出的字节数 x 域名组的 billing_coef), api 创建域名组时默认 1.05
	local ckb cb rt ttfb cms hc sc
	hc=$(billing_coef $DG)
	sc=$(billing_coef $SHARE_DG)
	ckb=$(ck <<<"SELECT sum(billing_bytes_sent_sum) FROM t_cdn_metrics WHERE $(stats_where) AND client_abort = 0")
	cb=$(awk -F'\t' -v h=$HOST -v hc="$hc" -v sc="$sc" '$5 == 0 {s += int($6 * ($1 == h ? hc : sc))} END{print s}' $T/stats.tsv)
	check "计费流量(不含中断的请求) = 每个请求客户端收到的字节数 x billing_coef($HOST $hc, $SHARE_HOST $sc)" "$ckb" "$cb"
	read -r rt ttfb <<<"$(ck <<<"SELECT sum(request_time_ms_sum), sum(ttfb_sum) FROM t_cdn_metrics WHERE $(stats_where) AND client_abort = 0")"
	cms=$(awk -F'\t' '$5 == 0 {s += $7} END{print s}' $T/stats.tsv)
	check "耗时(不含中断的请求): 0 < 首字节时间合计 <= 请求耗时合计 <= 客户端耗时合计" \
		"$(awk -v t="$ttfb" -v r="$rt" -v c="$cms" -v n="$n" 'BEGIN{print (0 < r && t <= r && r <= c + 2 * n) ? "ok" : "首字节 " t ", 请求 " r ", 客户端 " c}')" ok

	section "统计: api 接口($HOST, 只统计边缘层)"
	local d bytes hb mb
	d=$(stat_api request_count)
	check "request_count: 合计 $nh" "$(echo "$d" | jq '[.request_counts[].count // 0] | add')" "$nh"
	d=$(stat_api request_count "domain=$SHARE_HOST&granularity=1min")
	check "request_count($SHARE_HOST): 合计 $ns" "$(echo "$d" | jq '[.request_counts[].count // 0] | add')" "$ns"
	d=$(stat_api request_count "domain_group_unique_id=$DG&granularity=1min")
	check "request_count(按域名组 $DG): 合计 $nh" "$(echo "$d" | jq '[.request_counts[].count // 0] | add')" "$nh"
	d=$(stat_api request_count "domain=$HOST&granularity=5min")
	check "request_count(5 分钟粒度): 合计 $nh" "$(echo "$d" | jq '[.request_counts[].count // 0] | add')" "$nh"

	d=$(stat_api hit_rate)
	check "hit_rate: HIT / MISS 数" "$(echo "$d" | jq -r '[([.hit_rate_list[].hit_count // 0] | add), ([.hit_rate_list[].miss_count // 0] | add)] | join(" ")')" \
		"$(awk -F'\t' -v h=$HOST '$1 == h && $4 == "HIT" {a++} $1 == h && $4 == "MISS" {b++} END{print a + 0, b + 0}' $T/stats.tsv)"
	read -r hb mb <<<"$(ck <<<"SELECT sumIf(billing_bytes_sent_sum, cache_status = 'HIT'), sumIf(billing_bytes_sent_sum, cache_status = 'MISS')
		FROM t_cdn_metrics WHERE $(stats_where $HOST)")"
	check "hit_rate: HIT / MISS 流量与 ClickHouse 一致" \
		"$(near "$(echo "$d" | jq '[.hit_rate_list[].hit_traffic_gb // 0] | add * 1073741824')" "$hb" 1) $(near "$(echo "$d" | jq '[.hit_rate_list[].miss_traffic_gb // 0] | add * 1073741824')" "$mb" 1)" "ok ok"
	check "hit_rate: 每个点的流量命中率 = HIT 流量 / (HIT + MISS 流量)" \
		"$(echo "$d" | jq '[.hit_rate_list[] | select(.hit_traffic_gb != null) | (.hit_traffic_gb + .miss_traffic_gb) as $t
			| ((if $t > 0 then .hit_traffic_gb / $t * 100 else 0 end) - .traffic_hit_rate) | (. < 1e-9 and . > -1e-9)] | all')" true
	d=$(stat_api status_code_ratio)
	check "status_code_ratio: 各状态码请求数" "$(echo "$d" | jq -r '[.status_code_ratio_list[] | "\(.status_code):\(.request_count)"] | sort | join(" ")')" \
		"$(awk -F'\t' -v h=$HOST '$1 == h {n[$3]++} END{for (c in n) print c ":" n[c]}' $T/stats.tsv | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//')"
	d=$(stat_api error_rate)
	check "error_rate: 2xx / 4xx / 5xx 数" \
		"$(echo "$d" | jq -r '[([.error_rate_list[].http_2xx_count // 0] | add), ([.error_rate_list[].http_4xx_count // 0] | add), ([.error_rate_list[].http_5xx_count // 0] | add)] | join(" ")')" \
		"$(awk -F'\t' -v h=$HOST '$1 == h {c = substr($3, 1, 1); n[c]++} END{print n[2] + 0, n[4] + 0, n[5] + 0}' $T/stats.tsv)"
	d=$(stat_api client_abort_rate)
	check "client_abort_rate: 中断 1 个, 合计 $nh" \
		"$(echo "$d" | jq -r '[([.client_abort_rate_list[].client_abort_count // 0] | add), ([.client_abort_rate_list[].total_requests // 0] | add)] | join(" ")')" "1 $nh"
	d=$(stat_api region_isp_distribution)
	check "region_isp_distribution: 合计 $nh" "$(echo "$d" | jq .total_requests)" "$nh"

	bytes=$(ck <<<"SELECT sum(billing_bytes_sent_sum) FROM t_cdn_metrics WHERE $(stats_where $HOST)")
	d=$(stat_api traffic)
	check "traffic: 与 ClickHouse 的字节数一致" "$(near "$(echo "$d" | jq '[.traffic_list[].traffic_gb // 0] | add * 1073741824')" "$bytes" 1)" ok
	d=$(stat_api bandwidth)
	check "bandwidth: 字节数 x 8 / 60 秒(Gbps, 保留 10 位小数)" \
		"$(near "$(echo "$d" | jq '[.bandwidth_list[].bandwidth // 0] | add')" "$(awk -v b="$bytes" 'BEGIN{printf "%.12f", b * 8 / 60 / 1e9}')" 3e-10)" ok
	read -r rt ttfb <<<"$(ck <<<"SELECT sum(request_time_ms_sum), sum(ttfb_sum) FROM t_cdn_metrics WHERE $(stats_where $HOST)")"
	d=$(stat_api ttfb)
	check "ttfb: 首字节时间合计与 ClickHouse 一致" "$(echo "$d" | jq '[.ttfb_list[].total_ttfb_ms // 0] | add')" "$ttfb"
	d=$(stat_api request_time)
	check "request_time: 请求耗时合计与 ClickHouse 一致" "$(echo "$d" | jq '[.request_time_list[].total_request_time_ms // 0] | add')" "$rt"

	# 排行: 与上面各接口同一口径
	local n4 n5
	read -r n4 n5 <<<"$(awk -F'\t' -v h=$HOST '$1 == h {c = substr($3, 1, 1); n[c]++} END{print n[4] + 0, n[5] + 0}' $T/stats.tsv)"
	d=$(stat_api domain_top "domain=$HOST")
	check "domain_top: 请求数 $nh, 4xx $n4, 5xx $n5, 中断 1" \
		"$(echo "$d" | jq -r '.domain_top_list[0] | "\(.request_count) \(.http_4xx_count) \(.http_5xx_count) \(.client_abort_count)"')" "$nh $n4 $n5 1"
	check "domain_top: 平均首包 = 首包时间合计 / 请求数" \
		"$(near "$(echo "$d" | jq '.domain_top_list[0].average_ttfb_ms')" "$(awk -v t="$ttfb" -v n="$nh" 'BEGIN{printf "%.12f", t / n}')" 1e-9)" ok
	d=$(stat_api node_top "domain=$HOST")
	check "node_top: 边缘层只有 $EDGE_NODE, 请求数 $nh" "$(echo "$d" | jq -r '[.node_top_list[] | "\(.node_name):\(.request_count)"] | join(" ")')" "$EDGE_NODE:$nh"
	d=$(stat_api node_top "domain=$HOST&layer=source")
	check "node_top(layer=source): 回源只有 $ORIGIN_NODE" "$(echo "$d" | jq -r '[.node_top_list[].node_name] | join(" ")')" "$ORIGIN_NODE"
}

# ===========================================================================
echo "AresCDN 核心功能测试 run=$RUN edge=$EDGE host=$HOST groups=[$TEST_GROUPS]"

section "准备: 关闭跟随和分片"
follow_to 0
shard_to 0 -

for g in $TEST_GROUPS; do
	case $g in
	basic) test_basic ;;
	follow) test_follow ;;
	shard) test_shard ;;
	shard302) test_shard302 ;;
	share) test_share ;;
	errcode) test_errcode ;;
	fault) test_fault ;;
	stats) test_stats ;;
	*) echo "未知的组: $g" && exit 2 ;;
	esac
done

section "恢复: 关闭跟随和分片"
follow_to 0
shard_to 0 -

echo
echo -e "${B}结果: 通过 $PASS, 失败 $FAIL${N}"
for f in "${FAILED[@]}"; do echo -e "  ${R}FAIL${N} $f"; done
[ $FAIL -eq 0 ]
