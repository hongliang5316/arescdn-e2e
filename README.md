# arescdn-e2e

AresCDN 端到端测试：从边缘层发请求，经过回源层到测试源站，验证 edge、cache、cache-manager、api、dispatcher 配合后的实际行为；stats 组再验证统计链路（cache-manager → Kafka → arescdn-metric-consumer → ClickHouse → api 统计接口）。

```
curl(控制面机器) ──> 边缘层节点 ──> 回源层节点 ──> 测试源站(控制面机器 :8081)
                        │                │
                        └── cache ───────┴── cache
```

## 目录

| 路径 | 说明 |
|---|---|
| `run-tests.sh` | 测试脚本 |
| `setup.sh` | 初始化环境：生成测试文件、启动测试源站、创建测试域名组，可重复执行 |
| `lab.env.example` | 环境配置模板，`setup.sh` 复制成 `lab.env` 并填上域名组 ID（`lab.env` 不提交） |
| `origin/` | 测试源站：openresty 容器（host 网络），各种测试路径定义在 `nginx.conf` |
| `tools/cdnapi.sh` | 调用 AresCDN API，用 admin 的 token 签名鉴权 |
| `results/` | 建议把测试输出保存在这里（不提交） |

## 使用

在控制面机器（跑 api / mysql 容器的那台）上以 root 执行：

```bash
cp lab.env.example lab.env   # 按实际环境修改, 也可以直接执行 setup.sh 自动生成
sudo ./setup.sh
sudo ./run-tests.sh | tee results/$(date +%Y%m%d-%H%M%S).log
```

只跑其中几组：`sudo ./run-tests.sh shard shard302`。完整跑一遍约 22 分钟，大部分时间在等配置生效。

依赖：`curl`、`jq`、`python3`、`docker compose`。部分用例会访问外网：httpbin.org、mirrors.aliyun.com。stats 组要求本机能 `docker exec` 进 ClickHouse 容器（`CLICKHOUSE_CONTAINER`）。

## 测试环境

- 两个测试域名组，都绑定 `MLNPLOY` 节点组，回源到 `ORIGIN_IP:8081`：
  - `HOST`（"功能测试"）：大部分用例使用；
  - `SHARE_HOST`（"共享缓存测试"）：共享缓存域名（`as_domain`）为 `HOST`。
- 缓存规则：`/static/`、`/dyn/`、`/302/`、`/norange/` 缓存 1 小时；`/nocache/` 不缓存；其它路径按源站 `Cache-Control`，测试源站不返回这个头，所以不缓存。
- 测试源站：8081 是主源站，8082 模拟内网第三方地址（验证 302 跟随的内网拦截）。
  - 每个响应带 `X-Origin-Seq`（全局递增），同一 URL 两次序号相同说明命中了缓存。
  - 访问日志 `origin/logs/access.log` 记录来源 IP、Host、Range，脚本据此统计回源次数和分片区间。
- 脚本会通过 API 修改 `HOST` 的 302 跟随次数、分片大小，以及 `SHARE_HOST` 的共享缓存域名；每组结束时恢复本组的改动，退出时恢复默认（跟随、分片关闭，共享缓存域名为 `HOST`）。
- 统计里的流量是计费流量：每个请求 `floor(发出的字节数 × billing_coef)`。api 创建域名组时 `billing_coef` 默认为 1.05，所以统计流量比实际多 5%。
- 配置从修改到所有节点进程生效要等缓存过期（cache-manager 5 秒，edge 进程内和节点共享内存各 5 秒）。脚本探测到新配置生效后，再等 11 秒才继续。

## 测试内容

| 组 | 内容 |
|---|---|
| basic | 回源链路（来自回源层、Host 正确）；缓存规则（缓存 / 不缓存 / 无规则）；10MB 文件完整性；Range（开头、中间、末尾、首个请求就是 Range）；HEAD；URL 刷新、目录刷新、预热（内容更新，任务最终 Success，URL 刷新先刷回源层再刷边缘层） |
| follow | 302 跟随：关闭时原样返回；相对地址、本域名绝对地址；跟随结果被缓存；次数上限（等于、超过、配置超过 5 按 5）；循环；没有 Location；内网地址拦截；同源带 Cookie / Authorization，跳外网不带；301 / 307 / POST 不跟随，HEAD 跟随 |
| shard | 分片（512KB）：分片区间、每片只回源一次；Range 只回源需要的分片（片内、跨片、末尾、越界 416）；部分缓存后只补缺的分片；边界文件（整 2 片、1 片多 1 字节、小于 1 片、空文件）；源站不支持 Range；404；HEAD；不缓存的文件不分片；缓存部分分片后源站换成同样大小的新版本不拼接；修改分片大小只对新文件生效；关闭分片后旧文件仍可命中 |
| shard302 | 分片 + 302 跟随：跳到大文件后按分片回源、每片都重新跟随；各种 Range；多跳、超上限；本域名绝对地址；目标不支持 Range、目标 404；HEAD；跳到外网大文件（阿里云镜像）和 httpbin 小文件（越界 Range 返回 416 的不规范源站） |
| share | 共享缓存域名：两个域名互相命中、回源用各自的配置；Range；从任一域名提交 URL 刷新 / 目录刷新，另一个域名都更新；取消共享后各自缓存 |
| stats | 统计：发一批组成已知的请求（HIT / MISS、共享域名、404 / 206 / 416 / 302、HEAD / POST、10MB 文件、客户端中途断开），窗口内再做一次 URL 刷新和预热。ClickHouse 里按域名 / 方法 / 状态码 / 缓存状态 / 是否中断分组的请求数与实际一致；刷新预热不计入；节点、设备、域名组 ID、协议正确；回源层单独记录；计费流量等于客户端收到的字节数乘以 `billing_coef`；耗时合理。api 的 request_count（含 5 分钟粒度、按域名组、共享域名）、hit_rate（含 HIT / MISS 流量和流量命中率）、status_code_ratio、error_rate、client_abort_rate、region_isp_distribution、traffic、bandwidth、ttfb、request_time 与实际或 ClickHouse 一致 |

stats 组的做法：统计按分钟聚合，脚本等到新的一分钟开始才发请求，时间窗口按分钟对齐，窗口内只有本组的请求。所以执行期间不能有其它程序访问 `HOST` / `SHARE_HOST`。发完请求后等数据写进 ClickHouse，再多等 12 秒（每个 nginx worker 至少再上报一轮），这样多算的请求也会被发现。这一组约 2 分钟。

stats 组没有覆盖：https / QUIC（测试域名没有配证书，本机 curl 不支持 HTTP/3）、IPv6（环境没有）、`billing/export`（按月统计全部流量，无法隔离出本组的请求），以及 cache-manager、Kafka、ClickHouse 重启时的数据丢失和恢复。

另有两项只打印现象（INFO），不计入通过或失败：

- 对没缓存过的文件发 HEAD 时，源站收到的分片请求数（HEAD 会触发按分片把整个文件拉一遍来填充缓存）。
- 302 目标在两个大小相同、内容不同的文件之间交替时，分片会拼接成错误的文件。这是已知限制：同一次拉取中分片之间只校验文件总大小，决定以源站每次返回的跳转地址为准。

## 已知问题（与测试相关）

- 刷新明细、预热明细接口总是返回空：两张明细表没有写入方。
- 刚提交的刷新任务最多 1 秒内在任务列表里查不到：`create_time` 精确到秒且写入时四舍五入，查询截止时间是当前时间截断到秒。脚本会等几秒再判断任务状态。
