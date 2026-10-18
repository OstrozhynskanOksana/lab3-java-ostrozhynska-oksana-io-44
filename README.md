# Lab 3 — Stream API, Optionals

## Description

This project implements variant V=1: real-time IoT device telemetry. The
application processes potentially infinite `Stream<SensorMetric>` instances
with lazy Stream API operations, short-circuiting terminal operations,
`Optional`, and a custom stateful `Gatherer`.

## Variant calculation

The variant is calculated as the remainder of the student's record book
number divided by 3:

```text
V = record book number mod 3
```

For this project:

```text
4414 mod 3 = 1
```

Therefore, the implemented variant is V=1.

## Project structure

All application classes are located in the `ua.kpi.comsys.streams` package.

- `SensorMetric` is a record containing the device ID, metric type, value, and
  timestamp.
- `TelemetryStreamAnalyzer` contains the required telemetry operations.
- `TelemetryStreamAnalyzerTest` contains JUnit 5 tests for regular and
  potentially infinite streams.

## Implemented operations

### `findFirstAnomaly`

Filters metrics by device ID and returns the first metric whose value is
greater than the critical threshold. The result is returned as an
`Optional<SensorMetric>`.

### `streamActiveMetrics`

Skips the initial stable part of the stream, where metric values are less than
or equal to the normal threshold, and returns at most `maxCount` subsequent
metrics.

### `calculateMovingAverages`

Filters metrics by type and applies a custom sequential stateful `Gatherer`.
The gatherer stores the most recent `windowSize` values, calculates an average
after each matching metric, and stops after `outputLimit` results. This makes
the operation safe for matching infinite streams.

All required reference arguments are validated and cause
`IllegalArgumentException` when they are `null`.

## Build and test requirements

The project uses Gradle and the version catalog in
`gradle/libs.versions.toml`. JDK 24 or newer is required because
`java.util.stream.Gatherer` is part of the Java 24 Stream API.

The tests cover anomaly detection, stable-prefix skipping, moving averages,
metric-type filtering, null argument validation, infinite stream processing,
and short-circuiting behavior.

## Running tests

On Linux or macOS:

```bash
./gradlew test
```

On Windows PowerShell:

```powershell
.\gradlew.bat test
```

