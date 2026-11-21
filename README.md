# 双路 RTP 丢包补位中继（Dual-path RTP Relay）

直播链路在两路网络上同时传输同一节目（RTP 序号相同）。本服务从两个 UDP
端口收包，按 16 位可回绕序号去重、排序，把**原包**（不重封装、不改载荷）
依序转发给播出机；一路丢包时用另一路的同号包补位，两路都丢则在等待后跳过。

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 两个 UDP 端口收同一节目，Python asyncio | `app/udp_transport.py` 用 `create_datagram_endpoint` 在同一事件循环监听 A/B 两个端口，回调统一交给 `RelayEngine` |
| 以首包为起点 | 第一个有效 RTP 包的序号锚定为待发序号并立即转发，此前不缓存任何包 |
| 可回绕序号去重 | `app/rtp.py::seq_delta` 做模 2^16 前向距离比较；同序号首份保留，其余计入 `duplicates` |
| 原包依序转发 | 直接 `sendto` 收到的原始 datagram；引擎只在序号连续时出队，保证严格升序、无乱序 |
| 只接纳待发序号起 64 个位置内的包 | 前向距离 `d` 满足 `0 <= d < 64` 才入缓冲，`d >= 64` 计入 `out_of_window` 丢弃（窗口可用 `RELAY_WINDOW` 调） |
| 缺包等 80ms，从首个后续包到达起算，后续到包不延长 | 缓冲中出现队头缺口时，以**最早到包时间 + 80ms** 为截止时刻重新挂 `call_later` 定时器；新到包不改变截止时刻（`GAP_TIMEOUT_MS` 可调） |
| 两路静默，到期也跳过并继续 | 等待完全由事件循环定时器驱动，不依赖任何新输入；到期跳过缺号，之后到包继续按序转发 |
| FastAPI 查询最近 64 个跳过序号 | `GET /skipped` 返回有界 `deque(maxlen=64)`，新者在前；另有 `/status`、`/health` |
| 合流后按字节限速转发 | `app/forwarder.py` 字节令牌桶：按 RTP 原包长度计费、突发额度封顶、有界积压队列；不足则排队等额度，停流后仍由定时器排空 |
| Docker Compose 启动 + 本地演示 | `docker-compose.yml`（仅服务）、`docker-compose.demo.yml`（源→中继→播出机模拟一键演示）；无 Docker 时用 `demo/run_local_demo.sh` |

### 关键语义说明

- **补位与"恢复"计数**：若另一路的同号包在缺口成为*队头缺口*之前就已到达，
  引擎会在连续投递时直接用上，输出无缺号；只有在队头等待期间被补回的缺口
  才计入 `gaps_recovered`。两路同丢、等待超时才计入 `gaps_timed_out` 并进入
  `/skipped`。
- **跳过列表**：`deque(maxlen=64)` 保证最多保留最近 64 个跳过序号；
  `stats.skipped` 是不截断的累计计数。
- 非 RTP datagram（版本号≠2 或短于 12 字节）计入 `invalid` 并丢弃。

### 合流后限速（防止打满播出机入口）

补位时集中到达的包会按合流顺序、以字节令牌桶限速后再交给 UDP：

- **计费与内容**：按 RTP 原包 `len(data)` 计费，转发的是原始 datagram，
  不重封装、不改载荷；合流顺序严格不变。
- **额度**：`RELAY_RATE_BPS` 字节/秒持续补充，桶内最多存
  `RELAY_BURST_BYTES`。空闲或事件循环晚醒时补充的额度同样以突发额度封顶，
  不会出现"攒了一大笔额度后一次性爆发"。
- **排队**：额度不足整包时进入 FIFO，等到能整包发送再发；FIFO 不插队，
  后续更小的包也不能越过队头。定时器在队头恰好可发时唤醒，**上游停流后
  仍会继续排空**；等待额度期间照常收包、去重、走 80ms 跳号逻辑。
- **新包处理**：每次新包到达先按序发送"当前额度够用"的积压包，再决定新包
  直接发送还是排队。需要排队但 `queued + len(new) > RELAY_MAX_BACKLOG_BYTES`
  时**只丢弃这个新包**（计入 `rate_limit_drops`）；单包大于
  `RELAY_BURST_BYTES` 时永远无法发送，也直接丢弃。
- **计数口径**：`forwarded` 只统计真正成功交给 UDP 的包（`sendto` 抛错只计
  `send_errors`）；`rate_limit_drops` 是独立的限速丢包计数；`/skipped`
  与 `stats.skipped` 只表示**合流缺号**，限速丢弃绝不写入跳过列表。
  `/status` 额外给出 `backlog_bytes`、`backlog_packets`。
- `RELAY_RATE_BPS=0`（默认）关闭限速，保持原有不限速转发行为。
  `RELAY_MAX_BACKLOG_BYTES=0` 表示自动推导：取 `max(RELAY_BURST_BYTES,
  RELAY_RATE_BPS)`（约 1 秒额度），始终有限，不会无界排队。
  需要更严格的积压上限时显式设置（应不小于单个包长，否则该包只在额度
  立即可用时才发送，否则被丢弃）。

## 目录结构

```
app/
  config.py        环境变量配置与校验
  rtp.py           RTP 头解析、序号回绕运算、演示用建包
  relay.py         核心引擎：去重/窗口/80ms 等待/跳过/统计
  forwarder.py     合流后的字节令牌桶限速与有界待发队列
  udp_transport.py 双端口 ingress 与转发 socket
  api.py           FastAPI 路由
  main.py          单事件循环启动中继 + uvicorn
demo/
  demo_source.py   双路流量发生器（可配丢包/延迟/静默/瞬时突发）
  demo_sink.py     播出机侧接收器：缺口核对与收包间隔统计
  run_local_demo.sh 无 Docker 的本地一键演示
tests/             单元测试 + 真实 UDP/HTTP 端到端测试（含限速场景）
Dockerfile, docker-compose.yml, docker-compose.demo.yml
```

## 运行

### 方式一：Docker Compose（演示，推荐）

```bash
docker compose -f docker-compose.demo.yml up --build
```

三个容器共享网络命名空间：`source` 发两路有损流量 → `relay` 补位/跳过 →
`sink` 收恢复后的流并打印核对报告。中继持续运行，随后可查询：

```bash
curl -s localhost:8000/skipped
curl -s localhost:8000/status
```

演示场景：A 路丢 10/20/30，B 路丢 15/30，B 路的 10/40 延迟 40ms，
seq 50 后双路静默 300ms，seq 60 起瞬时连发 10 包（模拟补位集中到达）。
预期仅 **30** 进入跳过列表（两路同丢），其余全部补回；播出机侧
`intervals_ms` 显示 10 包突发被限速摊平（约 14ms 一包），`out_of_order`
为空，`relay_status.rate_limit_drops` 为 0（演示额度足够大，只平滑不丢弃）。
`intervals_ms.max` 会包含脚本刻意制造的 300ms 双路静默，属预期。

### 方式二：Docker Compose（仅服务，对接真实播出机）

```bash
FORWARD_HOST=10.0.0.10 FORWARD_PORT=9200 \
RELAY_RATE_BPS=1200000 RELAY_BURST_BYTES=144000 \
RELAY_MAX_BACKLOG_BYTES=600000 docker compose up --build
```

不设置三个 `RELAY_*` 变量时保持不限速（兼容原部署）。

### 方式三：本地 Python（无 Docker）

```bash
pip install -r requirements.txt
python -m app.main                      # 启动中继
./demo/run_local_demo.sh                # 另开终端，一键本地演示
```

端口冲突时可用环境变量覆盖：`API_PORT`、`UDP_PORT_A`、`UDP_PORT_B`、
`FORWARD_PORT`。

## API

| 路径 | 说明 |
| --- | --- |
| `GET /health` | 存活检查 |
| `GET /skipped` | `{"skipped": [..]}` 最近 64 个**合流缺号**，新者在前（不含限速丢包） |
| `GET /status` | 待发序号、缓冲长度、等待状态与截止剩余、限速器生效配置（`enabled`/`rate_bps`/`burst_bytes`/`max_backlog_bytes`）、`backlog_bytes`/`backlog_packets`、全部计数（含 `rate_limit_drops`）、跳过列表 |

## 配置（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `UDP_PORT_A` / `UDP_PORT_B` | 9100 / 9101 | 两路 ingress UDP 端口 |
| `FORWARD_HOST` / `FORWARD_PORT` | 127.0.0.1 / 9200 | 播出机地址 |
| `API_HOST` / `API_PORT` | 0.0.0.0 / 8000 | FastAPI 监听地址 |
| `RELAY_WINDOW` | 64 | 接纳窗口（位置数） |
| `GAP_TIMEOUT_MS` | 80 | 缺包等待毫秒数 |
| `RELAY_RATE_BPS` | 0 | 合流后转发字节速率（字节/秒）；0 = 不限速 |
| `RELAY_BURST_BYTES` | 0 | 突发额度（字节）；`RELAY_RATE_BPS>0` 时必须为正 |
| `RELAY_MAX_BACKLOG_BYTES` | 0 | 待发字节上限；0 = 自动取 max(突发额度, 1 秒速率) |

限速需 `RELAY_RATE_BPS>0` 且 `RELAY_BURST_BYTES>0`。示例（约 1000 个
1200 字节包/秒，允许 120ms 突发，最多积压 0.5s 的流量）：

```bash
RELAY_RATE_BPS=1200000 RELAY_BURST_BYTES=144000 \
RELAY_MAX_BACKLOG_BYTES=600000 docker compose up
```

## 自测

```bash
python -m unittest discover -s tests -v
```

- 单元测试覆盖：首包锚定、乱序重排、跨路去重首份优先、64 窗口边界、
  回绕距离、80ms 超时跳过、后续到包不延长等待、静默到期、临界补回、
  跳过列表 64 上限与新者在前、65535 回绕；以及限速器的突发额度、空闲额度
  封顶、晚醒封顶、FIFO 不插队、停流后定时排空、超突发单包直丢、积压满丢
  新包、发送失败只计 `send_errors`、`forwarded`/`rate_limit_drops`/`skipped`
  口径分离、配置校验（虚拟时钟驱动，时序确定）。
- 端到端测试用真实 UDP socket + 真实 uvicorn：跨路补位零跳过、双丢超时
  跳过、静默+回绕、窗口拒绝、`/skipped` 与接收侧缺口交叉核对；限速场景
  额外验证实际收包间隔被拉长、输出严格有序、积压随停流自行排空、
  限速丢弃不污染 `/skipped`。

## 已知边界 / 未完成项

- 本沙箱环境未安装 Docker，容器化文件已编写但**未在本机实际构建运行**；
  已用原生 Python 完成等价的端到端验证（含限速演示）。
- 限速在**单进程事件循环**内用 `call_later` 唤醒，队列在进程内；进程重启
  会丢失积压包。它不是 OS 级流量整形，`sendto` 成功后不做内核缓冲区回压
  监测；突发额度需按播出机入口承受能力配置。
- 未实现 SSRC 区分与 RTP 扩展头处理：默认两路为同一节目同一 SSRC；若两路
  SSRC 不同但序号同步，当前仍按序号合并（不校验 SSRC）。
- 未做 RTCP、拥塞控制与转发目的地址动态切换；`FORWARD_HOST` 为静态单播。
- 高码率下缓冲为进程内字典，无内存上限以外的持久化；进程重启会丢失上下文，
  以重启后首包重新锚定。
- 单进程单事件循环，未做多核/多实例水平扩展。
