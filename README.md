# parquet-to-csv

A generic CLI tool to convert Parquet files to CSV, built with Java 21 and PicoCLI.

Any Parquet schema is supported — primitive fields map directly to CSV columns, and
nested/complex fields (structs, LIST, MAP) are emitted as compact JSON in a single CSV
cell, with Parquet's `LIST`/`MAP` schema wrappers unwrapped so the JSON reflects the
user-intended shape (`["red","blue"]`, not `{"list":[{"element":"red"},…]}`). Use
`--skip-complex` to drop complex columns entirely for faster flat-data extraction.

No Hadoop runtime, no `HADOOP_HOME`, no `winutils.exe` — file I/O is pure Java NIO via
a custom `LocalInputFile`, so the fat JAR runs anywhere Java 21 runs.

---

## Requirements

- Java 21+
- Maven 3.8+ (to build)
- A UTF-8 capable terminal for the log banner icons (`┌─`, `│`, `✔`, `✘`).
  Logs are emitted as UTF-8 unconditionally; on **modern Windows Terminal,
  PowerShell 7, macOS, and Linux** this works out of the box. On legacy
  `cmd.exe` run `chcp 65001` once before launching the JAR, or pipe the
  output through a UTF-8-aware pager.

---

## Build

```bash
mvn package -DskipTests
```

Output: `target/parquet-to-csv-1.0.0.jar` — fat JAR, no external dependencies needed at runtime.

---

## Usage

```bash
java -jar target/parquet-to-csv-1.0.0.jar [OPTIONS] <input>...
```

### Arguments

| Argument  | Description                                                  |
|-----------|--------------------------------------------------------------|
| `<input>` | One or more `.parquet` files, or a directory containing them |

### Options

| Option                    | Default      | Description                                                                 |
|---------------------------|--------------|-----------------------------------------------------------------------------|
| `-o`, `--output-dir`      | —            | **Required.** Directory where CSV files will be written                     |
| `--overwrite`             | `false`      | Overwrite existing CSV files                                                |
| `--skip-complex`          | `false`      | Skip RECORD/LIST/MAP fields instead of emitting their Parquet text form     |
| `--delimiter <char>`      | `,`          | CSV column delimiter                                                         |
| `--null-value <string>`   | `""`         | String written for null values                                              |
| `--progress-interval <n>` | `100000`     | Log a progress line every N rows                                            |
| `-V`, `--version`         |              | Print version and exit                                                      |
| `-h`, `--help`            |              | Print help and exit                                                         |

---

## Examples

```bash
# Single file
java -jar parquet-to-csv-1.0.0.jar trades.parquet -o /output

# Multiple files
java -jar parquet-to-csv-1.0.0.jar file1.parquet file2.parquet -o /output

# Entire directory, overwrite existing CSVs
java -jar parquet-to-csv-1.0.0.jar /data/parquet-files/ -o /output --overwrite

# Skip nested fields for faster extraction of flat data
java -jar parquet-to-csv-1.0.0.jar /data/ -o /output --skip-complex

# Semicolon delimiter, custom null placeholder, progress every 50k rows
java -jar parquet-to-csv-1.0.0.jar file.parquet -o /output \
  --delimiter ';' --null-value "N/A" --progress-interval 50000
```

---

## Type mapping

Primitive Parquet physical types are written as typed strings. Complex types
(struct, LIST, MAP) are serialized as **compact JSON in a single CSV cell**,
with Parquet's `LIST`/`MAP` schema wrappers unwrapped so the JSON reflects the
user-intended shape, not Parquet's storage layout.

| Parquet physical type           | CSV output                                                       |
|---------------------------------|------------------------------------------------------------------|
| `INT32`                         | Plain integer string (e.g. `42`)                                 |
| `INT64`                         | Plain integer string (e.g. `1700000000000`)                      |
| `FLOAT`                         | `String.valueOf(float)` (e.g. `3.14`)                            |
| `DOUBLE`                        | `String.valueOf(double)` (e.g. `3.141592653589793`)              |
| `BOOLEAN`                       | `true` / `false`                                                 |
| `BINARY` (UTF8 strings, enums)  | The string contents via `group.getValueToString()`               |
| `BINARY` (raw bytes / decimal)  | Parquet's textual representation via `group.getValueToString()`  |
| `FIXED_LEN_BYTE_ARRAY`          | Textual representation via `group.getValueToString()`            |
| `INT96` (legacy timestamp)      | Parquet's textual representation via `group.getValueToString()`  |
| **Struct / RECORD**             | **JSON object** — `{"street":"Main St","city":"NYC"}`            |
| **LIST**                        | **JSON array** — `["red","blue"]` (3-level and 2-level layouts)  |
| **MAP**                         | **JSON object** — `{"env":"prod","region":"eu"}`                 |
| null / missing optional field   | Top-level: configured `--null-value`; inside JSON: `null`        |

Primitive values inside JSON keep their types — `INT32`/`INT64`/`FLOAT`/`DOUBLE` become JSON numbers,
`BOOLEAN` becomes a JSON boolean, everything else becomes a JSON string. Nested complex types
recurse, so a list of structs produces `[{"id":1,"name":"..."},{"id":2,"name":"..."}]`.

> **Logical types are not decoded.** A `DATE` column (stored as `INT32`) emits
> days-since-epoch as an integer, not an ISO-8601 string. A `TIMESTAMP_MILLIS`
> column emits raw epoch millis, not a formatted datetime. A `DECIMAL(p,s)`
> column emits its underlying physical bytes or integer, not a precision-aware
> decimal string. If you need logical-type interpretation, post-process the CSV
> or extend `primitiveToString(...)` in `ParquetConverter`.

> Use `--skip-complex` to drop struct/LIST/MAP columns entirely instead of
> emitting them as JSON. Useful when you only need flat scalar data and want
> maximum throughput.

---

## What the output looks like

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  parquet-to-csv  |  2 file(s) queued
  Output dir      : /output
  Overwrite       : false
  Delimiter       : ','
  Null value      : '<empty>'
  Progress every  : 100,000 rows
  Skip complex    : false
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[1/2] trades.parquet
┌─ Starting  : trades.parquet
│  Input     : /data/trades.parquet
│  File size : 142.3 MB
│  Columns   : 12 → [id, date, isin, quantity, ...]
│  Progress  : every 100,000 rows
  ↳ trades.parquet — 100,000 rows processed... (312,500 rows/sec, elapsed: 0.320s)
  ✔ trades.parquet — 147,832 rows written in 0.512s (288,734 rows/sec)
│  CSV size  : 89.7 MB
└─ Completed : trades.parquet in 0.512s

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  BATCH SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total time   : 1.1s
  Succeeded    : 2
  Failed       : 0
  Total rows   : 298,441

  Converted files:
    ✔ trades.csv    — 147,832 rows, 12 cols, 0.512s
    ✔ positions.csv — 150,609 rows,  8 cols, 0.601s
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## Performance notes

- The read loop is streaming — row group → page → record, one record at a time.
  No full-file buffering: heap usage stays flat regardless of file size.
- Primitive columns are read via typed accessors (`getInteger`, `getLong`, `getDouble`, …),
  which is as fast as the Parquet low-level API allows.
- The main cost for complex schemas is the per-row `Group.toString()` call used to
  serialize nested/list/map fields. Use `--skip-complex` to eliminate that cost entirely
  when you only need flat scalar columns.
- Throughput on a flat schema with SSD storage is typically **200k–400k rows/sec**.
  Deeply nested schemas with many complex fields will be slower.
- The rows/sec figure shown in progress logs is your real-time performance indicator.

---

## Project structure

```
parquet-to-csv/
├── pom.xml
└── src/main/java/com/tizitec/parquet2csv/
    ├── Main.java               Entry point — wires PicoCLI and exits with its return code
    ├── ConvertCommand.java     CLI definition (@Command, @Option, @Parameters) and orchestration
    ├── ConversionConfig.java   Immutable config record — bridges CLI layer and converter
    ├── ParquetConverter.java   Core conversion logic — generic, schema-driven
    ├── LocalInputFile.java     Pure Java NIO implementation of Parquet's InputFile —
    │                           replaces HadoopInputFile so no Hadoop FS is needed
    └── ProgressLogger.java     Row-count progress and throughput logging utility
└── src/main/resources/
    └── logback.xml             Logging config — suppresses Hadoop/Parquet noise
```

---

## Dependencies

| Library                        | Version | Purpose                                                                    |
|--------------------------------|---------|----------------------------------------------------------------------------|
| picocli                        | 4.7.6   | CLI framework                                                              |
| parquet-hadoop                 | 1.14.3  | `ParquetFileReader` (low-level reader API, schema-driven)                  |
| parquet-column                 | 1.14.3  | Column/page reading primitives used by `ParquetFileReader`                 |
| parquet-jackson                | 1.14.3  | Parquet's shaded Jackson — required internally to parse the file footer   |
| hadoop-common                  | 3.4.1   | API-level only: parquet-hadoop imports Hadoop classes from its signatures  |
| hadoop-mapreduce-client-core   | 3.4.1   | Same — classes referenced from parquet-hadoop's public API                 |
| commons-csv                    | 1.12.0  | CSV writing                                                                |
| jackson-databind               | 2.17.2  | JSON serialization of complex Parquet fields (struct / LIST / MAP)         |
| zstd-jni                       | 1.5.5-11| ZSTD decompression (default codec for Spark/Iceberg)                       |
| aircompressor                  | 0.27    | LZ4 decompression (pure Java, used by older Parquet writers)               |
| snappy-java (transitive)       | 1.1.10  | Snappy decompression — pulled in via hadoop-common                         |
| slf4j-api                      | 2.0.13  | Logging API                                                                |
| log4j-over-slf4j               | 2.0.13  | Routes Hadoop/Parquet's log4j calls through SLF4J                          |
| logback-classic                | 1.5.8   | SLF4J backend                                                              |

> **Note on Hadoop:** the Hadoop artifacts are on the classpath because
> `parquet-hadoop` exposes Hadoop types in its public API (e.g. `Configuration`,
> `FileInputFormat`) — their classes must be loadable. All heavy transitive deps
> (Jetty, Jersey, Kerby, ZooKeeper, Curator, etc.) are excluded in `pom.xml`, and
> no Hadoop runtime is ever instantiated. File I/O goes through `LocalInputFile`,
> which is pure Java NIO.

---

## Exit codes

| Code | Meaning                                                       |
|------|---------------------------------------------------------------|
| `0`  | All files converted successfully                              |
| `1`  | One or more files failed (remaining files are still processed)|
