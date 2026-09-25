#!/bin/bash
# 调用 AresCDN API: cdnapi.sh METHOD PATH [JSON_BODY] [额外查询参数]
#   例: cdnapi.sh GET /api/cdn/v1/domaingroups
#       cdnapi.sh PATCH /api/cdn/v1/domaingroups '{"unique_id":"cdn-xxx","acc_follow302_max":3}'
#
# 用 API 自带的签名鉴权: signature = md5(yyyymmdd + username + token), 日期按 +08:00.
# 需要在控制面机器上以 root 执行: 从 $ARESTECH_DIR/.env 读 MySQL 密码, 用 docker exec 查用户的 token.
set -e

ARESTECH_DIR=${ARESTECH_DIR:-/opt/arestech}
API_ADDR=${API_ADDR:-http://127.0.0.1:18080}
API_USER=${API_USER:-admin}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-arescdn-mysql}

M=$1
P=$2
BODY=${3:-}
Q=${4:-}

MYSQL_ROOT_PASSWORD=$(grep '^MYSQL_ROOT_PASSWORD=' "$ARESTECH_DIR/.env" | cut -d= -f2-)
TOKEN=$(docker exec -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" "$MYSQL_CONTAINER" \
	mysql -uroot -N -B arescdn -e "select token from t_cdn_user where name=\"$API_USER\"" 2>/dev/null)
SIG=$(echo -n "$(TZ=Asia/Shanghai date +%Y%m%d)${API_USER}${TOKEN}" | md5sum | cut -d" " -f1)

SEP="?"
[[ "$P" == *"?"* ]] && SEP="&"
URL="${API_ADDR}${P}${SEP}username=${API_USER}&signature=${SIG}${Q:+&$Q}"

if [ -n "$BODY" ]; then
	curl -s -X "$M" -H "Content-Type: application/json" -d "$BODY" "$URL"
else
	curl -s -X "$M" "$URL"
fi
echo
