## Mercury 项目

### 概览

Mercury 是一个以模块化 Maven 多模块仓库组织的 Java 库/中间件集合，聚焦于并发、持久化、序列化与网络传输等基础设施能力。项目使用 Java 21，包含若干供上层服务复用的子模块。

主要特性（概要）
- 多模块 Maven 项目（aggregator / 子模块按功能划分）
- Java 21
- 使用 Lombok、Jackson、Netty、Akka（部分模块）、多种序列化实现（Avro/Kryo/Protobuf/SBE/JSON）

模块及子模块说明
----------------
（按目录结构与 pom.xml 列出，并给出简短用途描述）

Top-level modules
- commons
    - commons-concurrent: 并发工具/组件集合（线程池、并发数据结构等，供其它模块复用）。
    - commons-core: 基础核心 util/通用类型（项目内部通用功能）。
    - commons-graph: 可能与图/关系数据结构有关的工具（基于目录名推断）。
    - commons-reflect: 反射/辅助工具（类型/方法/字段操作封装）。

- libraries
    - library-encryption: 加密相关工具/封装（例如对称/非对称/工具类）。
    - library-nacos: Nacos 客户端/集成（配置/服务发现相关）。

- persistence
    - persistence-chronicle: Chronicle 等内存/零拷贝持久化集成（高性能序列化存储）。
    - persistence-h2: 基于 H2 的嵌入式关系型持久化实现（用于测试或轻量持久层）。
    - persistence-rocksdb: RocksDB 本地键值存储集成（高性能磁盘存储）。
    - persistence-sqlite: SQLite 持久化支持（轻量嵌入式 DB）。
      注：persistence 模块在 dependencyManagement 中管理了诸如 HikariCP、commons-dbutils、commons-compress、lz4、zero-allocation-hashing、netty-buffer、joda-time 等常用库。

- serialization
    - serialization-avro: Avro 序列化/反序列化支持。
    - serialization-fory: Fory（项目内部或第三方）序列化实现（按目录名推断）。
    - serialization-json: 基于 Jackson 的 JSON 序列化支持。
    - serialization-kryo: Kryo 二进制序列化支持。
    - serialization-protobuf: Protobuf 支持。
    - serialization-sbe: SBE （Simple Binary Encoding）支持（一般需要 codegen 步骤）。
      注：serialization 模块的 dependencyManagement 管理了大量 Jackson 相关模块、Netty 模块与压缩/Hash 库（commons-compress、lz4、snappy、zstd 等）。

- transport
    - transport-aeron: 基于 Aeron 的高性能传输层实现。
    - transport-netty: 基于 Netty 的传输实现与封装。
    - transport-rabbitmq: RabbitMQ 消息传输集成。
    - transport-rsocket: RSocket 传输实现。
    - transport-ws: WebSocket 传输（HTTP/WebSocket）。
    - transport-zmq: ZeroMQ（zmq）传输集成。
      注：transport 的 dependencyManagement 指定了 Lombok 版本并且将部分 serialization 子模块版本纳入管理。

单独模块
- actors: 基于 Akka（Scala 3 / Akka 2.10.x）实现的 actor/流/cluster/HTTP 支持（包含 akka-actor、akka-stream、akka-cluster-tools、akka-http 等依赖，且排除了若干 slf4j/scala 库以便项目使用托管版本）。

依赖（汇总）
-----------
以下依赖与版本主要来自顶层 `pom.xml` 的 properties 以及各个子模块的 dependencyManagement。若某条依赖在子模块中未显式声明版本，则通常由父级或 dependencyManagement 管理。

核心平台与工具
- Java: 21（pom.properties 中 <java.version>21</java.version>）
- Maven: 建议使用 Maven 3.8+（构建测试时）

在父级（mercury/pom.xml）声明的关键版本
- slf4j.version = 2.0.17
- junit.version = 4.13.2
- jupiter.version = 5.14.1
- netty.version = 4.2.10.Final
- jackson.version = 2.21.1
- guava.version = 33.5.0-jre
- lombok（在子模块 dependencyManagement 中常见）= 1.18.42

persistence 模块中管理的一些第三方库（及版本）
- jakarta.persistence-api: 3.1.0
- jakarta.transaction-api: 2.0.1
- commons-dbutils: 1.8.1
- HikariCP: 6.3.0
- commons-compress: 1.27.1
- lz4-java: 1.8.0
- zero-allocation-hashing: 0.16
- netty-buffer: ${netty.version}
- joda-time: 2.14.0

serialization 模块中管理的库
- jackson-databind / jackson-core / jackson-annotations / jackson-datatype-* : ${jackson.version} / 部分声明为 2.21
- netty-buffer / netty-codec / netty-handler: ${netty.version}
- zero-allocation-hashing: 0.16
- 还有 snappy / zstd 等压缩 JNI 库通过 properties 管理（snappy-java 1.1.10.7, zstd-jni 1.5.7-1）

transport 模块中管理（示例）
- lombok: 1.18.42
- 内部引用了多个 serialization 模块（sbe/avro/json）作为依赖

actors 模块关键依赖
- akka.version: 2.10.6
- akka-http: 10.6.3
- scala3-library_3: 3.5.0
- asm-tree: 9.7

如何构建与测试
----------------
前提
- 安装 JDK 21 并配置 JAVA_HOME 指向 JDK21
- 安装 Maven（推荐 3.8+）

常用命令
- 全模块构建（会构建父 pom 中声明的聚合模块）：
  mvn clean install

- 跳过测试的快速构建：
  mvn -DskipTests clean install

- 构建单个模块并同时构建其依赖（按 artifactId）：
  mvn -pl :serialization-json -am clean package
  mvn -pl :transport-netty -am clean package

- 仅运行测试：
  mvn test
  mvn -pl actors test

开发与调试注意事项
------------------
- Java 版本：项目使用 Java 21（pom.xml 中 <java.version>21）。请确保 IDE 使用的 Project SDK / Language level 为 21。
- Lombok：多个模块使用 Lombok（常见版本 1.18.42）。在 IDE（IntelliJ IDEA / Eclipse）中启用注解处理器并安装 Lombok 插件以正确识别生成的代码。
- Preview 特性：部分模块的编译插件配置包含 compilerArgument 如 `--enable-preview`（见 commons 模块），如果代码使用了 JDK preview 特性，需要在运行/测试 VM options 中添加 `--enable-preview`。
- Akka/Scala：`actors` 模块使用 Akka 与 Scala（scala3-library_3），构建时需要兼容 Scala 版本的构建环境，并且项目 pom 已明确排除了 scala 的一些传递依赖以使用集中管理的版本。
- 本地构建速度：建议使用并行构建：
  mvn -T 1C clean install

代码生成（如果适用）
- serialization-sbe 这类模块通常需要运行 codegen（SBE 工具等）生成 Java 类。请查看相应模块的 pom.xml 中的 plugin 配置（或 README）以执行生成步骤。

常见仓库与镜像
- 官方 Maven Central
- Akka 使用专门仓库：https://repo.akka.io/maven/

贡献与联系方式
----------------
- 项目源（父 pom 指向）：https://github.com/Cygnux-Ltb/basic
- 许可证：见项目目录下 LICENSE 文件
- 贡献：建议通过 fork + pull request 流程，开 issue 或在 PR 中附上复现步骤与单元测试。

附注与限制
----------------
- 本 README 为自动生成，基于当前工作区的 pom.xml 与有限的 README 内容汇总；某些模块的详细用途与实现细节未从源代码逐文件提取，建议在需要深入理解时阅读对应模块的 src 代码或模块 README（若存在）。
- 某些依赖的精确使用场景（例如哪个模块直接引用了哪一版本的 jackson 模块）需要更细粒度的 pom 解析；README 中若标注为 `${property}` 或 "inherited/managed"，请以实际构建解析结果为准。

生成/验证
---------
此文件已生成为 `README_GENERATED.md`（位于项目根）。

---