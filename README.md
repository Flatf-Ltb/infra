# Infra
Created: 2020-01-03 15:50 (JST)
Last updated: 2026-06-04 18:43 (JST)

`infra` is the shared infrastructure library repository for the Flatf workspace. It contains
persistence, serialization, transport, and integration helper modules used by trading services,
adapters, and local development tools.

## Maven Coordinates

- Parent coordinates: `io.flatf:infra:1.0.1`
- Packaging: `pom`
- Java version: `21`
- Repository: `https://github.com/Flatf-Ltb/infra.git`

## Aggregated Modules

The root `pom.xml` currently aggregates these modules:

- `libraries`
- `persistence`
- `serialization`
- `transport`

`actors` exists in the source tree, but it is not currently included in the root Maven reactor.

## Module Summary

### libraries

- `library-encryption`: encryption helpers and wrappers based on Bouncy Castle.
- `library-nacos`: Nacos client and configuration/service-discovery integration support.

### persistence

- `persistence-chronicle`: Chronicle Queue and Chronicle Map persistence integration.
- `persistence-h2`: H2 embedded relational persistence support.
- `persistence-rocksdb`: RocksDB local key-value persistence support.
- `persistence-sqlite`: SQLite embedded persistence support.

### serialization

- `serialization-avro`: Avro serialization support and generated message models.
- `serialization-fory`: Fory binary serialization support.
- `serialization-json`: Jackson and Fastjson based JSON serialization support.
- `serialization-kryo`: Kryo binary serialization support.
- `serialization-protobuf`: Protobuf serialization support.
- `serialization-sbe`: SBE code generation and serialization support.

### transport

- `transport-aeron`: Aeron transport support.
- `transport-netty`: Netty transport support.
- `transport-rabbitmq`: RabbitMQ transport support.
- `transport-rsocket`: RSocket transport support.
- `transport-socket`: socket transport support.
- `transport-ws`: WebSocket transport support.
- `transport-zmq`: ZeroMQ transport support.

## Repository Boundary

`infra` depends on the separate `commons` repository for shared common utilities such as
`io.flatf:commons-core` and `io.flatf:commons-concurrent`.

- Keep low-level utility code in `repos/commons`.
- Keep persistence, serialization, transport, and integration infrastructure in `repos/infra`.
- Do not reintroduce a local `commons/` source tree under this repository.

Infra-owned Java packages use the `io.flatf.infra.*` namespace. Imports from `repos/commons` still
use the current commons package namespaces, including `io.flatf.common.*` and shared
transport base abstractions such as `io.flatf.foundation.transport.api.*`.

## Build

Run Maven through the workspace wrapper from the workspace root:

```powershell
.\tools\Invoke-Maven.ps1 -WorkingDirectory .\repos\infra -DskipTests compile
```

If `commons-*` artifacts are not present in the local Maven repository, install `repos/commons`
first:

```powershell
.\tools\Invoke-Maven.ps1 -WorkingDirectory .\repos\commons -DskipTests install
```

Then build `infra`:

```powershell
.\tools\Invoke-Maven.ps1 -WorkingDirectory .\repos\infra -DskipTests compile
```
