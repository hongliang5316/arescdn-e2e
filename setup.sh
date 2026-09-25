#!/bin/bash
# 初始化测试环境, 可重复执行: sudo ./setup.sh
#   1. 生成测试文件(已存在则跳过)
#   2. 启动测试源站容器
#   3. 查找或创建测试域名组 HOST / SHARE_HOST, 设置缓存规则, SHARE_HOST 的共享缓存域名设为 HOST
#   4. 把域名组 ID 写入 lab.env
set -eu

BASE=$(cd "$(dirname "$0")" && pwd)
if [ ! -f "$BASE/lab.env" ]; then
	cp "$BASE/lab.env.example" "$BASE/lab.env"
	echo "已从 lab.env.example 生成 lab.env, 请确认其中的环境配置"
fi
set -a
. "$BASE/lab.env"
set +a

API=$BASE/tools/cdnapi.sh
O=$BASE/origin

# ---------------------------------------------------------------------------
echo "== 生成测试文件"
mkdir -p "$O/static/shard" "$O/logs" "$BASE/results"
gen() { # gen 文件 字节数: 随机内容
	[ -f "$1" ] || head -c "$2" /dev/urandom >"$1"
}
[ -f "$O/static/small.txt" ] || echo small-file >"$O/static/small.txt" # 11 字节
gen "$O/static/10m.bin" 10485760
gen "$O/static/shard/exact-1m.bin" 1048576 # 正好 2 个 512KB 分片
gen "$O/static/shard/plus1.bin" 524289     # 1 个分片多 1 字节
gen "$O/static/shard/small-100k.bin" 102400
[ -f "$O/static/shard/empty.bin" ] || : >"$O/static/shard/empty.bin"
# flip-a / flip-b: 大小相同、修改时间相同(ETag 也相同)、内容不同, 用于"分片之间的一致性"
if [ ! -f "$O/static/shard/flip-a.bin" ] || [ ! -f "$O/static/shard/flip-b.bin" ]; then
	head -c 1048576 /dev/urandom >"$O/static/shard/flip-a.bin"
	head -c 1048576 /dev/urandom >"$O/static/shard/flip-b.bin"
	touch -r "$O/static/shard/flip-a.bin" "$O/static/shard/flip-b.bin"
fi

# ---------------------------------------------------------------------------
echo "== 启动测试源站"
(cd "$O" && docker compose up -d)
for _ in $(seq 1 10); do
	curl -s -o /dev/null "http://127.0.0.1:8081/" && break
	sleep 1
done
echo "8081: $(curl -s http://127.0.0.1:8081/)"

# ---------------------------------------------------------------------------
echo "== 测试域名组"
api_ok() { # api_ok METHOD PATH JSON: 调用失败时退出
	local resp
	resp=$("$API" "$@")
	if [ "$(echo "$resp" | jq -r .errno)" != 0 ]; then
		echo "API 调用失败: $* => $resp" >&2
		exit 1
	fi
	echo "$resp"
}

# find_dg 域名: 输出包含该域名的域名组 ID
find_dg() {
	"$API" GET /api/cdn/v1/domaingroups |
		jq -r --arg d "$1" '.data.domain_group_list[]? | select(.domain_list | index($d)) | .unique_id' | head -1
}

# ensure_dg 名称 域名 说明: 查找或创建域名组, 输出 ID
ensure_dg() {
	local id
	id=$(find_dg "$2")
	if [ -z "$id" ]; then
		id=$(api_ok POST /api/cdn/v1/domaingroups "{
			\"name\": \"$1\", \"mlnploy_unique_id\": \"$MLNPLOY\", \"domain_list\": [\"$2\"],
			\"status\": \"enable\", \"tag\": \"test\", \"info\": \"$3\",
			\"origin\": {
				\"protocol\": \"http\",
				\"origin_info_list\": [{\"host\": \"$ORIGIN_IP\", \"port\": 8081, \"weight\": 1, \"type\": \"ipv4\"}],
				\"timeouts\": {\"connect_timeout\": 3, \"send_timeout\": 8, \"read_timeout\": 8, \"idle_timeout\": 120}
			}
		}" | jq -r .data.unique_id)
		echo "创建域名组 $1($2): $id" >&2
	else
		echo "域名组 $1($2) 已存在: $id" >&2
	fi
	echo "$id"
}

# 缓存规则: /static/ /dyn/ /302/ /norange/ 缓存 1 小时, /nocache/ 不缓存, 其它路径按源站 Cache-Control(测试源站不返回, 即不缓存)
set_cache_rules() {
	api_ok PATCH /api/cdn/v1/domaingroups/cache "{
		\"unique_id\": \"$1\",
		\"rule_list\": [
			{\"rule_type\": \"directory\", \"rule_path_list\": [\"/static/\", \"/dyn/\", \"/302/\", \"/norange/\"],
			 \"cache_option\": {\"type\": \"cache\", \"time\": 1, \"unit\": \"h\"}, \"priority\": 10},
			{\"rule_type\": \"directory\", \"rule_path_list\": [\"/nocache/\"],
			 \"cache_option\": {\"type\": \"no_cache\"}, \"priority\": 20}
		]
	}" >/dev/null
}

DG=$(ensure_dg "功能测试" "$HOST" "核心功能测试, 源站 $ORIGIN_IP:8081")
SHARE_DG=$(ensure_dg "共享缓存测试" "$SHARE_HOST" "共享缓存域名测试: 共享 $HOST 的缓存")
set_cache_rules "$DG"
set_cache_rules "$SHARE_DG"
api_ok PATCH /api/cdn/v1/domaingroups "{\"unique_id\": \"$DG\", \"acc_follow302_max\": 0, \"acc_sharding_size_str\": \"0\"}" >/dev/null
api_ok PATCH /api/cdn/v1/domaingroups "{\"unique_id\": \"$SHARE_DG\", \"as_domain\": \"$HOST\"}" >/dev/null

sed -i -e "s/^DG=.*/DG=$DG/" -e "s/^SHARE_DG=.*/SHARE_DG=$SHARE_DG/" "$BASE/lab.env"
echo "已写入 lab.env: DG=$DG SHARE_DG=$SHARE_DG"
echo "配置同步到节点需要十几秒, 之后可以执行: sudo ./run-tests.sh"
