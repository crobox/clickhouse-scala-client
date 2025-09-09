# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build and Test
- **Build project**: `sbt compile`
- **Run unit tests**: `sbt test`
- **Run integration tests**: `sbt IntegrationTest/test`
- **Run both test suites**: `sbt "test; IntegrationTest/test"`
- **Run specific Scala version**: `sbt ++2.13.16 test` or `sbt ++3.3.6 test`

### Code Quality
- **Format code**: `sbt scalafmtAll` (uses scalafmt with preset defaultWithAlign)
- **Check formatting**: `sbt scalafmtCheck`

### Project Structure
- **Cross-compile**: This project supports Scala 2.13.16 and 3.3.6
- **Multi-module**: Three main modules: `client`, `dsl`, `testkit`

## Architecture Overview

This is a Clickhouse Scala client built on Apache Pekko (Akka fork) with reactive streams support.

### Core Modules

**client/** - Core Clickhouse client implementation
- `ClickhouseClient` - Main client class for query execution and streaming
- `HostBalancer` - Connection balancing with health checks (single-host, multi-host, cluster-aware)
- `ClickhouseSink` - Pekko Streams sink for data insertion
- Uses Pekko HTTP for async operations and streaming
- Supports query progress monitoring (experimental)

**dsl/** - Type-safe query DSL
- Composable DSL for building Clickhouse queries
- Column functions organized by category (aggregation, array, date/time, etc.)
- Schema builder for DDL operations
- Query execution integration with the client

**testkit/** - Testing utilities
- `ClickhouseSpec` - Base spec for integration tests with automatic database lifecycle
- Test matchers and utilities
- Creates temporary databases for isolated testing

### Key Features
- Reactive streaming with Pekko Streams
- Multiple connection types with automatic failover
- Query retrying with host balancing
- Streaming inserts and result parsing
- Type-safe query building via DSL
- Comprehensive test coverage (unit + integration)

### Configuration
All configuration is under `crobox.clickhouse.client` and `crobox.clickhouse.indexer` prefixes. See reference.conf in client module and README for detailed options.

### Integration Tests
Requires Docker and Clickhouse running. Uses docker-compose setup in `.docker/` directory with SSL certificates and multiple Clickhouse versions (22.8, 23.3, 23.8) for CI testing.