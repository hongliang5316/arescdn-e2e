#!/bin/bash
# AresCDN 故障演练: 停掉 CDN 的组件, 看请求怎么表现, 然后恢复.
#
# 在能 ssh 到所有机器的地方执行(本机), 不是在控制面机器上:
#   ./drill.sh all
#   ./drill.sh cache-down layer2-down
#   ssh ubuntu 'cat /root/git/arestech/arescdn-e2e/drill/drill.sh' | bash -s -- all
#
# 场景(all 按这个顺序全部执行):
#   cache-down          边缘层节点的 cache 进程挂了
#   layer2-down         回源层节点整个挂了(nginx 停掉)
#   cache-manager-down  边缘层节点的 cache-manager 挂了
#   redis-down          控制面的 Redis 挂了
#   meta-corrupt        边缘层节点上一个缓存对象的 meta 损坏(cache 应淘汰后回源)
#
# 会短暂停止服务: 不要在有真实流量、或者 e2e / arescdn-load 运行时执行.
# 脚本里的 ssh 都带 -n: 通过管道把脚本交给 bash 时, ssh 会读走标准输入里还没执行的脚本.
# 每个场景结束时恢复; 中途退出时由 EXIT 钩子恢复.
set -u

EDGE_HOST=${EDGE_HOST:-edge01}     # 边缘层节点的 ssh 别名
EDGE_IP=${EDGE_IP:-192.168.3.67}   # 请求发往的边缘层节点
LAYER2_HOST=${LAYER2_HOST:-edge02} # 回源层节点的 ssh 别名
CTRL_HOST=${CTRL_HOST:-api-web-db} # 控制面(docker compose), 请求也从这里发出
HOST=${HOST:-test.a.qq.com}        # 测试域名, 回源到测试源站
REDIS_CONTAINER=${REDIS_CONTAINER:-arescdn-redis}
# edge 的域名配置: 每个 worker 的 lrucache 和节点共享内存各缓存 5 秒, cache-manager 再缓存 5 秒
STALE_WAIT_SECS=${STALE_WAIT_SECS:-20}

SCENARIOS="cache-down layer2-down cache-manager-down redis-down meta-corrupt"
[ $# -eq 0 ] && {
	sed -n '2,20p' "$0" 2>/dev/null | sed 's/^# \{0,1\}//'
	exit 2
}
[ "$*" = all ] && set -- $SCENARIOS

RUN=$(date +%s)
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
between() { awk -v v="$1" -v lo="$2" -v hi="$3" 'BEGIN{if (v >= lo && v <= hi) print "ok"; else print v}'; }

# 退出时执行的恢复命令(后进先出), 场景正常结束时用 restored 去掉
RESTORE=()
on_exit() { RESTORE+=("$1"); }
restored() { unset 'RESTORE[${#RESTORE[@]}-1]'; }
restore_all() {
	local i
	for ((i = ${#RESTORE[@]} - 1; i >= 0; i--)); do
		echo -e "  ${Y}恢复${N} ${RESTORE[$i]}"
		eval "${RESTORE[$i]}"
	done
}
trap restore_all EXIT

# req 路径: 从控制面机器经边缘层请求, 输出 "状态码 缓存状态 耗时(秒)"
req() {
	ssh -n $CTRL_HOST "curl -s -o /dev/null -D - -m 30 -w 'TIME %{time_total}\n' -H 'Host: $HOST' 'http://$EDGE_IP$1'" |
		tr -d '\r' | awk '/^HTTP/{c = $2} tolower($1) == "x-cache-status:"{s = $2} /^TIME/{t = $2} END{print (c ? c : "-"), (s ? s : "-"), t}'
}
code() { req "$1" | cut -d' ' -f1; }
# attempts 路径: 边缘层 nginx 最近一次处理这个路径时向 cache 请求了几次($upstream_addr 里的地址数)
attempts() {
	ssh -n $EDGE_HOST "sudo grep -F '$1' /data/log/nginx/cdn-access.log | tail -1" |
		awk -F'\t' '{print split($(NF-7), a, ", ")}'
}
svc() { ssh -n "$1" "sudo systemctl $2 $3 && sleep 2" >/dev/null; }

# ===========================================================================
drill_cache_down() {
	section "cache 进程挂了($EDGE_HOST)"
	local p="/static/small.txt?r=drill-cache-down-$RUN"
	on_exit "svc $EDGE_HOST start arescdn-cache"
	svc $EDGE_HOST stop arescdn-cache
	check "返回 502" "$(code "$p")" 502
	check "nginx 连不上 cache 后换设备重试(每个节点只有 1 台时, 重试的还是它)" "$(attempts "$p")" "[2-9]"
	svc $EDGE_HOST start arescdn-cache
	restored
	check "cache 恢复后返回 200" "$(code "$p")" 200
}

drill_layer2_down() {
	section "回源层节点挂了($LAYER2_HOST 的 nginx 停掉)"
	local hit="/static/small.txt?r=drill-l2-hit-$RUN" miss="/static/small.txt?r=drill-l2-miss-$RUN"
	code "$hit" >/dev/null
	check "准备: 边缘层已缓存一个文件" "$(req "$hit" | cut -d' ' -f1-2)" "200 HIT"
	on_exit "svc $LAYER2_HOST start arescdn-edge"
	svc $LAYER2_HOST stop arescdn-edge
	check "已缓存的内容照常命中" "$(req "$hit" | cut -d' ' -f1-2)" "200 HIT"
	set -- $(req "$miss")
	check "没缓存的返回 502" "$1" 502
	check "很快返回(连不上回源层, 不用等超时)" "$(between "$3" 0 2)" ok
	svc $LAYER2_HOST start arescdn-edge
	restored
	check "回源层恢复后返回 200" "$(code "$miss")" 200
}

drill_cache_manager_down() {
	section "cache-manager 挂了($EDGE_HOST)"
	on_exit "svc $EDGE_HOST start arescdn-cache-manager"
	svc $EDGE_HOST stop arescdn-cache-manager
	info "等 $STALE_WAIT_SECS 秒, 超过 edge 各级配置缓存的时间"
	sleep $STALE_WAIT_SECS
	check "没缓存的请求仍然正常(edge 继续用旧配置)" "$(code "/static/small.txt?r=drill-cm-$RUN")" 200
	svc $EDGE_HOST start arescdn-cache-manager
	restored
	check "cache-manager 恢复后正常" "$(code "/static/small.txt?r=drill-cm-after-$RUN")" 200
}

drill_redis_down() {
	section "Redis 挂了($CTRL_HOST 的 $REDIS_CONTAINER)"
	on_exit "ssh -n $CTRL_HOST 'sudo docker start $REDIS_CONTAINER' >/dev/null"
	ssh -n $CTRL_HOST "sudo docker stop $REDIS_CONTAINER" >/dev/null
	info "等 $STALE_WAIT_SECS 秒, 超过 cache-manager 和 edge 的配置缓存时间"
	sleep $STALE_WAIT_SECS
	check "没缓存的请求仍然正常(两层都用旧配置)" "$(code "/static/small.txt?r=drill-redis-$RUN")" 200
	ssh -n $CTRL_HOST "sudo docker start $REDIS_CONTAINER" >/dev/null
	restored
	sleep 3
	check "Redis 恢复后正常" "$(code "/static/small.txt?r=drill-redis-after-$RUN")" 200
}

drill_meta_corrupt() {
	section "缓存对象的 meta 损坏($EDGE_HOST)"
	local p="/static/small.txt?r=drill-meta-$RUN" m
	ssh -n $EDGE_HOST "sudo touch /tmp/drill-meta-marker"
	sleep 1
	code "$p" >/dev/null
	check "准备: 边缘层已缓存一个文件" "$(req "$p" | cut -d' ' -f1-2)" "200 HIT"
	m=$(ssh -n $EDGE_HOST "sudo find /data/cache -name meta -newer /tmp/drill-meta-marker")
	check "找到这个文件的 meta(1 个)" "$(echo "$m" | grep -c .)" 1
	[ "$(echo "$m" | grep -c .)" = 1 ] || return
	# cache 每 60 秒把 LRU 索引写盘一次, 关闭时不写. 写盘前重启的话, 新进程不认识这个文件, 直接当作未命中
	info "等 LRU 索引写盘(最多 65 秒), 否则重启后 cache 不认识这个文件"
	ssh -n $EDGE_HOST "for i in \$(seq 1 65); do [ -n \"\$(sudo find /data/cache -maxdepth 2 -name lru_db -newer /tmp/drill-meta-marker)\" ] && exit 0; sleep 1; done; exit 1"
	check "LRU 索引已写盘" "$?" 0
	ssh -n $EDGE_HOST "sudo python3 -c 'import json, sys; p = sys.argv[1]; d = json.load(open(p)); d[\"response_header_bytes\"] = \"AAAA\"; json.dump(d, open(p, \"w\"), indent=2)' $m"
	on_exit "ssh -n $EDGE_HOST \"curl -s -o /dev/null -X PURGE -H 'Host: $HOST' 'http://127.0.0.1$p'\""
	info "改坏 meta 里的响应头, 重启 cache 清掉内存里的 meta"
	svc $EDGE_HOST restart arescdn-cache
	check "cache 发现 meta 损坏, 淘汰后回源重新缓存(cache prerelease-13 起)" "$(req "$p" | cut -d' ' -f1-2)" "200 MISS"
	check "cache 日志: 发现 meta 损坏" \
		"$(ssh -n $EDGE_HOST "sudo grep -F 'drill-meta-$RUN' /data/log/arescdn-cache/arescdn-cache.log | grep -c 'corrupted meta'")" "[1-9]*"
	check "之后正常命中" "$(req "$p" | cut -d' ' -f1-2)" "200 HIT"
	eval "${RESTORE[${#RESTORE[@]}-1]}"
	restored
}

# ===========================================================================
echo "AresCDN 故障演练 run=$RUN edge=$EDGE_IP host=$HOST scenarios=[$*]"
for s in "$@"; do
	case $s in
	cache-down) drill_cache_down ;;
	layer2-down) drill_layer2_down ;;
	cache-manager-down) drill_cache_manager_down ;;
	redis-down) drill_redis_down ;;
	meta-corrupt) drill_meta_corrupt ;;
	*) echo "未知的场景: $s" && exit 2 ;;
	esac
done

echo
echo -e "${B}结果: 通过 $PASS, 失败 $FAIL${N}"
# bash 3.2(macOS 自带)在 set -u 下展开空数组会报错
for f in ${FAILED[@]+"${FAILED[@]}"}; do echo -e "  ${R}FAIL${N} $f"; done
[ $FAIL -eq 0 ]
