# legacy-src (persistence-chronicle)
Created: 2026-10-09 14:20 (JST)
Last updated: 2026-10-09 14:20 (JST)

旧架构参考源码 (不参与编译)。`persistence-chronicle` 模块于 **2026-10-07 退出构建**
(父 pom `persistence/pom.xml` 的 `<modules>` 不再聚合, `rapid/pom.xml` 的 dependencyManagement
条目同日移除), 本目录是原 `src/` 的原样归档, 是该模块除 `pom.xml` 外的唯一剩余内容。

这是 `repos/infra` 下第一个 `legacy-src` 目录; 约定与 `repos/rapid` 的各模块一致 ——
永不编译, 不得删除, 不要直接恢复进构建。

## 为什么退出

1. **全仓零引用。** Rapid 的 core / engine / platform / adapter / gateway 没有任何一处
   `import net.openhft` 或 `io.flatf.infra.persistence.chronicle`。`rapid/pom.xml` 里只在
   `dependencyManagement` 锁了版本, 没有任何模块真的 declare 过它 —— 即编译、打包, 但无人使用。
2. **依赖是 early-access 版本。** `chronicle-queue 5.27ea11`、`chronicle-map 3.27ea2`。
   EA artifact 不保证长期留在 Maven Central, 也不保证 API 稳定到 GA。
3. **一半以上是上游 demo 代码。** 见下表: 56 个 main 源文件里 31 个不是自有代码。
   与已删除的 vendored ta4j 同类问题。

## 目录内容

| 子树 | 文件数 | 性质 |
|---|---|---|
| `main/java/io/flatf/` | 25 | **自有代码** —— 对 Chronicle 的封装 |
| `main/java/net/openhft/` | 9 | Chronicle 官方教程样例 (`queue.simple.input` / `queue.simple.translator`) |
| `main/java/town/lost/` | 22 | Peter Lawrey 的 OMS 与事件处理演示 (`town.lost` 是其 demo 包命名空间) |
| `main/resources/` | 1 | `user.avsc` |
| `test/java/` | 6 | 其中 1 个是上游样例的测试 (`SimpleTranslatorTest`) |
| `test/resources/` | 4 | `town.lost.oms` 演示用的 YAML in/out 夹具 |

自有代码 (`io/flatf/infra/persistence/chronicle/`) 三块:

- `hash/` (10) —— ChronicleMap/Set 封装: `ChronicleMapKeeper` 及按日期、按 LRU 的两个变体,
  `ChronicleHashStorage`, `AdjustableChronicleMap`/`AdjustableChronicleSet`, 两个 Configurator
- `queue/` (12) —— ChronicleQueue 封装: `AbstractChronicleQueue`/`Appender`/`Reader` 三件套,
  Bytes/Document/String 三种具体队列, `multitype/` 多类型 JSON 队列, `FileCycle`, `ReaderParams`
- `exception/` (3) —— Append / IO / Read 三个异常类型

`net/openhft/` 与 `town/lost/` 两棵树**不是自有代码**, 复用前请核对上游许可与出处, 不要当作
本工作区的设计参考。

## 如果将来要恢复这个能力

它本来要覆盖的能力 —— **可持久化、可重放的事件日志** —— 至今仍是缺口, 且是真实缺口:
Disruptor 环是纯内存, CDX/ZMQ 通道是发完即忘, 引擎崩溃即丢失当日事件流 (无审计、无复盘、
无法用实盘数据回测)。

恢复前先确认两件事:

1. **量级。** CTP 行情在少量合约上是每秒数百条, 而 Chronicle Queue 与 Aeron Archive 都是按
   百万条/秒设计的。按天分文件的 append-only 日志、甚至直接落 SQLite, 可能就够, 且零新依赖。
2. **载体。** 若确实需要高性能载体, 优先 **Aeron Archive** 而非 Chronicle Queue ——
   `transport-aeron` 已依赖 `aeron-all` + `aeron-cluster` 1.50.4, 而 Aeron 的 Archive
   (持久化/重放) 与 Cluster (HA) 都在 Apache 2.0 范围内; Chronicle 的跨主机复制在 Enterprise
   许可墙内, 切口恰好落在交易系统迟早要回答的"引擎挂了怎么办"上。

## 未受影响的其他 net.openhft 依赖

本次退出**只针对 chronicle-queue / chronicle-map**。以下仍在正常使用, 不要一并清理:

- `zero-allocation-hashing` —— `persistence/pom.xml` 的 dependencyManagement, 由
  `persistence-rocksdb` 使用
- `chronicle-network`、`chronicle-ticker` —— `transport/transport-socket`
