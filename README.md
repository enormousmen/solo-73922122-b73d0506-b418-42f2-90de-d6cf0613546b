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
| Docker Compose 启动 + 本地演示 | `docker-compose.yml`（仅服务）、`docker-compose.demo.yml`（源→中继→播出机模拟一键演示）；无 Docker 时用 `demo/run_local_demo.sh` |

### 关键语义说明

- **补位与"恢复"计数**：若另一路的同号包在缺口成为*队头缺口*之前就已到达，
  引擎会在连续投递时直接用上，输出无缺号；只有在队头等待期间被补回的缺口
  才计入 `gaps_recovered`。两路同丢、等待超时才计入 `gaps_timed_out` 并进入
  `/skipped`。
- **跳过列表**：`deque(maxlen=64)` 保证最多保留最近 64 个跳过序号；
  `stats.skipped` 是不截断的累计计数。
- 非 RTP datagram（版本号≠2 或短于 12 字节）计入 `invalid` 并丢弃。

## 目录结构

```
app/
  config.py        环境变量配置与校验
  rtp.py           RTP 头解析、序号回绕运算、演示用建包
  relay.py         核心引擎：去重/窗口/80ms 等待/跳过/统计
  udp_transport.py 双端口 ingress 与转发 socket
  api.py           FastAPI 路由
  main.py          单事件循环启动中继 + uvicorn
demo/
  demo_source.py   双路流量发生器（可配丢包/延迟/静默）
  demo_sink.py     播出机侧接收器与缺口核对
  run_local_demo.sh 无 Docker 的本地一键演示
tests/             17 个单元测试 + 4 个真实 UDP/HTTP 端到端测试
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
seq 50 后双路静默 300ms。预期仅 **30** 进入跳过列表（两路同丢），其余全部补回。

### 方式二：Docker Compose（仅服务，对接真实播出机）

```bash
FORWARD_HOST=10.0.0.10 FORWARD_PORT=9200 docker compose up --build
```

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
| `GET /skipped` | `{"skipped": [..]}` 最近 64 个跳过序号，新者在前 |
| `GET /status` | 待发序号、缓冲长度、等待状态与截止剩余、全部计数、跳过列表 |

## 配置（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `UDP_PORT_A` / `UDP_PORT_B` | 9100 / 9101 | 两路 ingress UDP 端口 |
| `FORWARD_HOST` / `FORWARD_PORT` | 127.0.0.1 / 9200 | 播出机地址 |
| `API_HOST` / `API_PORT` | 0.0.0.0 / 8000 | FastAPI 监听地址 |
| `RELAY_WINDOW` | 64 | 接纳窗口（位置数） |
| `GAP_TIMEOUT_MS` | 80 | 缺包等待毫秒数 |

## 自测

```bash
python -m unittest discover -s tests -v
```

- 单元测试覆盖：首包锚定、乱序重排、跨路去重首份优先、64 窗口边界、
  回绕距离、80ms 超时跳过、后续到包不延长等待、静默到期、临界补回、
  跳过列表 64 上限与新者在前、65535 回绕。
- 端到端测试用真实 UDP socket + 真实 uvicorn：跨路补位零跳过、双丢超时
  跳过、静默+回绕、窗口拒绝、`/skipped` 与接收侧缺口交叉核对。

## 已知边界 / 未完成项

- 本沙箱环境未安装 Docker，容器化文件已编写但**未在本机实际构建运行**；
  已用原生 Python 完成等价的端到端验证。
- 未实现 SSRC 区分与 RTP 扩展头处理：默认两路为同一节目同一 SSRC；若两路
  SSRC 不同但序号同步，当前仍按序号合并（不校验 SSRC）。
- 未做 RTCP、拥塞控制与转发目的地址动态切换；`FORWARD_HOST` 为静态单播。
- 高码率下缓冲为进程内字典，无内存上限以外的持久化；进程重启会丢失上下文，
  以重启后首包重新锚定。
- 单进程单事件循环，未做多核/多实例水平扩展。
