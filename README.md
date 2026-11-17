# 固定内网区域主备 DNS（FastAPI + SQLite + dnslib）

为固定内网区域 `internal.local`（可用 `DNS_ZONE` 修改）提供主备两台权威 DNS：

- **主机 (primary)**：通过 REST API 接收一次提交、最多 8 条 A 记录的**完整区域**；
  仅在整份区域校验并落库成功后才把序号 (serial) 加 1。发布不依赖备用机。
- **备用机 (backup)**：从主机 HTTP 拉取**整份版本**，支持手动同步、后台自动同步和
  版本查询；两机各自使用独立 SQLite 数据库（不同文件/卷）。
- 两机都通过 **UDP 53 风格端口（默认 5053）** 回答区域内 A 查询，每个回答只取自
  **一份不可变的完整版本**快照，绝不混版。
- **主机隔离**：值班员可对某个**已发布的 A 名称**提交 30～300 秒的临时隔离，
  隔离期间主备两机对该名的 A 查询返回 **SERVFAIL**，同区域其他名称照常解析；
  隔离不改动区域序号，随正常同步通道下发到备用机，到期按原截止时间自动解除。

## 设计要点与需求对应

| 需求 | 实现方式 |
| --- | --- |
| 单次提交 ≤8 条 A 记录的完整区域 | `app/models.py`：非空、≤8、仅 IPv4、名称必须在固定区域内、禁止重名 |
| 发布成功才递增序号 | `Storage.next_publish()`：单事务 `BEGIN IMMEDIATE` 内插入新版本并切换 active 指针；校验失败在进事务前返回 422，序号不变 |
| 两机独立数据库 | 各自 `DB_PATH`（compose 中 `primary-data` / `backup-data` 两个卷） |
| 单次回答只用一份完整版本 | `zone_versions` 存不可变整份快照；DNS 解析前一次性读取 active 版本，再在内存中查表回答 |
| 手动同步 + 版本查询 | `POST /api/sync`、`GET /api/version`、`GET /api/zone` |
| 慢传/断传期间继续回答旧版 | 传输失败（连接拒绝、超时、HTTP 错误、坏报文）不触碰 active 版本，旧版本继续服务；同步状态写入 `state` 表 |
| 断开/重启后自动追到最新版 | 备用机启动即拉取一次，之后按 `SYNC_INTERVAL` 后台循环，直到与上游同序号 |
| 迟到旧传输不得回退 | `commit_version()` 拒绝任何 `serial <= active_serial` 的版本（返回 `ignored_stale`/False），事务保护 |
| 任一方失败仍供原版 | 发布失败保留旧 active；同步失败保留旧 active；DNS 与 HTTP 在同一进程内独立运行 |
| 主机发布无需等待备用 | 主机 API 不调用备用机；e2e 中备用停机时发布仍在毫秒级成功 |
| Docker Compose 双服务 + Swagger | `docker-compose.yml` 起 primary/backup；`http://<host>:8080/docs`、`http://<host>:8081/docs` |
| **隔离提交** | `POST /api/isolation`（仅主机）：`serial`（所见序号）+ `name`（已发布 A 名）+ `request_id`（幂等键）+ `duration`（30–300 秒）；全部校验与落库在一个 `BEGIN IMMEDIATE` 事务内 |
| **同一时刻只隔离一个名称** | `state.current_isolation` 单槽位；槽位被生效中的隔离占用时新请求返回 409 |
| **同一请求重试不能续期** | `isolations.request_id` 主键；同 id 同参数重试返回 200 与原截止时间（不续期），同 id 不同参数返回 409 |
| **隔离不改序号也同步到备用** | 隔离状态随 `GET /api/zone` 响应携带；备用同序号时只调和隔离槽（`apply_isolation`），新序号时版本+隔离同事务提交（`commit_version`） |
| **断流/重启后按原截止时间解除** | 截止时间为绝对时间戳 `expires_epoch`，持久化在 SQLite；各机用自己的时钟判定到期，无需联系上游 |
| **发布/接纳新区使旧隔离立即失效** | `next_publish()` 与 `commit_version()` 在同一事务里清隔离指针；且生效判定要求 `isolation.serial == active_serial`，旧隔离永远罩不住新区 |
| **应答与版本接口状态匹配** | `get_active_state()` 在**一个读事务**内取回 (serial, records, published_at, isolation)；DNS 应答、`/api/version`、`/api/zone`、`/api/isolation` 都基于该快照 |
| **提交/同步失败各机按本机状态应答** | 提交失败事务回滚不动现状；同步失败不触碰本机版本与隔离槽 |

### 数据模型（SQLite，WAL 模式）

```
zone_versions(serial INTEGER PRIMARY KEY, records_json TEXT,
              record_count INTEGER, published_at TEXT)   -- 不可变整份版本
state(key TEXT PRIMARY KEY, value TEXT)
        active_serial      当前生效版本指针
        current_isolation  当前隔离槽位 -> isolations.request_id
        last_sync_*        备用机最近一次同步状态（at/status/error）
        upstream_serial    最近看到的上游序号
isolations(request_id TEXT PRIMARY KEY, name TEXT, serial INTEGER,
           duration INTEGER, created_at TEXT, expires_at TEXT,
           expires_epoch REAL)                          -- 隔离请求（含历史，供幂等）
```

版本切换 = 插入新版本行 + 更新 `active_serial` 指针，在同一个事务里提交，
因此外部观察到的永远是“某一份完整版本”，不存在中间态。
隔离**生效**当且仅当：槽位指针存在、`isolation.serial == active_serial`、
且 `now < expires_epoch`。三个条件任一不满足，该名照常解析。

## 端口与 API

| 角色 | API/Swagger | UDP DNS |
| --- | --- | --- |
| primary | 主机 8080 → 容器 8000 | 主机 5053 → 容器 5053/udp |
| backup  | 主机 8081 → 容器 8000 | 主机 5054 → 容器 5053/udp |

- `GET  /health` 健康检查
- `GET  /api/version` 角色、当前序号、记录数、生效中的隔离、备用机同步状态
- `GET  /api/zone` 当前完整区域（含 serial、records、published_at、isolation）
- `POST /api/zone` **仅主机**，请求体 `{"records":[{"name":"db.internal.local","ip":"10.0.0.11"}]}`，成功返回 201 与新 serial
- `POST /api/sync` **仅备用机**，立即拉取一次，返回 `synced` / `isolation_updated` / `ignored_stale` / `failed`
- `POST /api/isolation` **仅主机**，提交主机隔离（见下）
- `GET  /api/isolation` 两机均可，查看本机当前生效的隔离（含剩余秒数）

### 主机隔离 API

```json
POST /api/isolation
{
  "serial": 3,                       // 值班员所见区域序号（须等于当前 active 序号）
  "name": "db.internal.local",       // 已发布的 A 名称
  "request_id": "op-20261117-0001",  // 请求标识（幂等键）
  "duration": 120                    // 30～300 秒
}
```

- `201 created`：隔离已生效，响应含绝对截止时间 `expires_at`；
- `200 duplicate`：同一 `request_id` 同参数重试，返回原隔离、**截止时间不变**；
- `409`：序号与当前不符 / 已有其他隔离生效 / `request_id` 被不同参数占用；
- `404`：该名不在当前区域的 A 记录中；`422`：期限越界或参数非法；`403`：备用机不接受提交。

隔离期间：主备两机对该名的 **A 查询返回 SERVFAIL**；该名的非 A 查询仍 NOERROR/NODATA，
其他名称照常解析，未知名称仍 NXDOMAIN，区域外仍 REFUSED。到期后两机各自按
原截止时间自动恢复，无需任何人工干预或上游联系。

## 运行方式

### Docker Compose（推荐）

```bash
docker compose up --build
# 主机 Swagger:  http://localhost:8080/docs
# 备用 Swagger:  http://localhost:8081/docs

curl -sX POST http://localhost:8080/api/zone -H 'content-type: application/json' \
  -d '{"records":[{"name":"db.internal.local","ip":"10.0.0.11"},
                  {"name":"web.internal.local","ip":"10.0.0.21"}]}'

# 手动同步（后台也会每 3 秒自动同步）
curl -sX POST http://localhost:8081/api/sync

# 隔离 db.internal.local 120 秒（serial 以 /api/zone 实际值为准）
curl -sX POST http://localhost:8080/api/isolation -H 'content-type: application/json' \
  -d '{"serial":1,"name":"db.internal.local","request_id":"op-0001","duration":120}'

# 查看本机生效中的隔离
curl -s http://localhost:8080/api/isolation
curl -s http://localhost:8081/api/isolation

# UDP 查询
dig @127.0.0.1 -p 5053 db.internal.local A    # 隔离期间 -> SERVFAIL
dig @127.0.0.1 -p 5054 web.internal.local A   # 备用机，其他名照常
```

### 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv --without-pip   # 或普通 venv
.venv/bin/pip install -r requirements.txt
DNS_ROLE=primary DB_PATH=./data/primary.db DNS_PORT=15053 API_PORT=18000 \
  .venv/bin/python -m uvicorn app.main:app --port 18000
DNS_ROLE=backup  DB_PATH=./data/backup.db  DNS_PORT=15054 API_PORT=18001 \
  PRIMARY_API_URL=http://127.0.0.1:18000 \
  .venv/bin/python -m uvicorn app.main:app --port 18001
```

## 测试

```bash
.venv/bin/python -m pytest tests/test_unit.py -q     # 校验/事务/防回退/隔离存储 13 项
.venv/bin/python -I tests/e2e_test.py                # 双进程端到端 30 项
.venv/bin/python -I tests/auto_sync_test.py          # 自动同步/幂等/Swagger 8 项
.venv/bin/python -I tests/isolation_test.py          # 隔离端到端 47 项
```

端到端覆盖：启动空版本 NXDOMAIN、非法发布不升序号、发布后 UDP 应答、
手动与自动同步、黑洞上游期间旧版持续服务、恢复后自动追平、
迟到旧版本注入被拒且不回退、进程重启持久化与追赶、备用停机时主机照常发布。

隔离端到端覆盖：主备 SERVFAIL 与其他名称/类型不受影响、序号不变、
隔离随同步下发、重试不续期、单槽 409、序号/名称/期限校验、
备用重启后隔离仍在且截止时间不变、到期两机自动解除、
断流期间备用按本机状态维持并在原截止时间解除、
发布立即使旧隔离失效、旧请求重试不能重新罩住新区。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DNS_ROLE` | `primary` | `primary` 或 `backup` |
| `DNS_ZONE` | `internal.local` | 固定内网区域名 |
| `DB_PATH` | `/data/dns.db` | 本机独立 SQLite 文件 |
| `DNS_PORT` / `API_PORT` | `5053` / `8000` | UDP DNS / HTTP 端口 |
| `PRIMARY_API_URL` | `http://primary:8000` | 备用机指向上机 API |
| `SYNC_INTERVAL` | `5` | 备用机后台同步间隔秒 |
| `HTTP_TIMEOUT` | `10` | 拉取区域 HTTP 超时秒 |
| `MAX_RECORDS` | `8` | 单次区域最大 A 记录数 |

## 已知边界

- 仅支持 A 记录与 IPv4（题目范围）；区域固定为一个，区域名通过配置设定。
- 传输为整份区域拉取（无增量/AXFR 分帧）；区域上限 8 条，单份报文很小。
- 单机内嵌一个 UDP 解析线程与一个 uvicorn 进程；未做水平扩展与 TSIG 鉴权，
  内网部署时应限制 `/api/zone`、`/api/isolation` 的调用来源。
- 隔离期限固定 30～300 秒，到期自动解除；无人工提前解除接口（重新发布一个
  新版本也会立即使隔离失效）。
- 备用机的隔离状态以最近一次成功同步为准；同步中断期间按本机已生效状态应答，
  恢复后随下一次同步收敛（同序号下的隔离调和也是幂等的）。

