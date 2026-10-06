# otlp_slo227

纯后端 OTLP 增量（DELTA）指标转累积抓取服务。接收 OTLP/Protobuf 指标，
按时间区间累加为累积态，并以 Prometheus 0.0.4 文本格式暴露。

## 构建与启动

```sh
go build -o otlp_slo227 .
./otlp_slo227
```

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OTLP227_PORT` | `8080` | 本机监听端口 |
| `OTLP227_MAX_SERIES` | `10000` | 序列上限，超限拒绝新序列（不驱逐活动序列） |
| `OTLP227_EXPIRY_SECONDS` | `300` | 空闲过期秒数，过期释放容量 |

## 接口

- `POST /v1/metrics`：OTLP/Protobuf `ExportMetricsServiceRequest`。仅支持
  DELTA 单调 Sum 与带 sum 的显式桶 DELTA Histogram。整批原子提交，任一数据点
  非法则整批拒绝（400），不留部分状态；超限返回 429。
- `GET /metrics`：Prometheus 0.0.4 文本。Sum 导出 `<name>_total`，Histogram
  导出累积 `_bucket`（含 `+Inf`）、`_sum`、`_count`，标签正确转义。

## 语义

- 序列由资源 `service.instance.id`、作用域名、指标名与字符串点属性标识
  （属性顺序无关），导出 `instance`、`scope` 标签；点属性禁用
  `instance`/`scope`/`le`。指标名与属性键须符合 Prometheus 传统 ASCII
  规则。同名异类型拒绝。
- 区间为非零纳秒且开始早于结束。首段初始化；开始等于上一结束则累加；晚于则
  重置；上一段原样重传不累加、不延寿；异内容重传、其他重叠或倒退拒绝。
- 桶边界有限递增且同序列不可变，桶数为边界数加一，非负桶计数之和等于 count；
  Sum 非负有限；计数与和溢出拒绝。
- 并发抓取只见完整快照；空闲序列过期后释放容量，不驱逐活动序列。

## 演示

```sh
# 终端 1
OTLP227_PORT=18080 ./otlp_slo227

# 终端 2：上报两个相邻增量区间（重传同段不累加）
go run ./cmd/genpayload -kind sum -name http_requests -value 5 -start 1000 -end 2000 | \
  curl -X POST localhost:18080/v1/metrics -H 'Content-Type: application/x-protobuf' --data-binary @-
go run ./cmd/genpayload -kind sum -name http_requests -value 7 -start 2000 -end 3000 | \
  curl -X POST localhost:18080/v1/metrics -H 'Content-Type: application/x-protobuf' --data-binary @-

# 抓取累积结果：http_requests_total 为 12
curl localhost:18080/metrics
```

## 测试

```sh
go test ./...
```
