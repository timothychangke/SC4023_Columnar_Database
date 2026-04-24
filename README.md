SC4023 — Big Data Management: Semester Group Project

Our column-oriented in-memory database engine written in C++17 for querying Singapore HDB resale flat transaction records (2015–2025). The engine implements **15 toggleable optimisations** spanning five architectural layers and is benchmarked across **50+ configurations** with automated correctness verification.

**Group members:**

| Name | Matric No. |
|------|-----------|
| Timothy Chang | U2220136J |
| Neo Zhi Xuan | U2222293E |

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Building](#building)
- [Running](#running)
- [Optimisation Flags](#optimisation-flags)
- [Recommended Configurations](#recommended-configurations)
- [Evaluation Suite](#evaluation-suite)
- [Query Parameter Derivation](#query-parameter-derivation)
- [Output Format](#output-format)
- [Error Handling](#error-handling)

---

## Overview

This program ingests the `ResalePricesSingapore.csv` dataset (259,237 records) and answers the following query:

> For each valid `(x, y)` pair — where `x` is a window of months (1–8) and `y` is a minimum floor area in m² (80–150) — find the HDB resale record with the **minimum price per square metre** within a filtered set of towns and time window. A pair is valid if that minimum price per square metre is **at most 4725 SGD/m²**.

Query parameters (target year, start month, and towns) are derived from a matriculation number provided at runtime. For our submission matric `U2220136J`, this yields: year **2016**, start month **3 (March)**, towns **{CLEMENTI, BEDOK, BUKIT PANJANG, CHOA CHU KANG, PASIR RIS}**, producing **568** valid `(x, y)` pairs.

---

## Project Structure

```
Project/
├── Makefile                        # Build targets: make, make run, make eval
├── main.cpp                        # Entry point — CLI parsing, orchestration
├── include/
│   ├── column_store.h              # ColumnStore struct, DictionaryEncoder, zone maps, RLE metadata
│   ├── csv_parser.h                # CSV ingestion declarations
│   ├── query_engine.h              # QueryResult struct, runQuery, buildCumulativeTable
│   ├── output_writer.h             # CSV output writer declaration
│   └── column_file_io.h            # Binary column file I/O, mmap, partitioned I/O 
├── src/
│   ├── column_store.cpp            # ColumnStore::size(), ::clear(), memory estimation
│   ├── csv_parser.cpp              # CSV parsing, dict encoding, pre-sorting, PPSM, RLE
│   ├── query_engine.cpp            # Query parameter derivation, scan logic, all query-path optimisations
│   ├── output_writer.cpp           # writeResults() — ScanResult CSV generation
│   └── column_file_io.cpp          # Column file read/write, mmap, chunked I/O, partitioned files
├── eval/
│   └── eval_suite.cpp              # Benchmarking harness — 50+ configs, correctness verification
├── data/
│   └── columns*/                   # Generated binary column files (created by --write-columns)
└── results/
    └── EvalResult_*.txt            # Evaluation suite output files
```

---

## Architecture

### Column-Oriented Storage

Data is stored in a strict **column store** — there is no `Row` or `Record` object anywhere. Each attribute lives in its own `std::vector`, all kept at equal length (parallel alignment):

```
col_month_year[i]          col_month_month[i]         col_town[i]
col_block[i]               col_street_name[i]         col_flat_type[i]
col_flat_model[i]          col_storey_range[i]        col_floor_area[i]
col_lease_commence_date[i] col_resale_price[i]
```

Index `i` across every vector always refers to the same logical record.

### Data Type Choices

| Column | C++ Type | Size | Rationale |
|--------|----------|------|-----------|
| Month year | `uint16_t` | 2 B | 4-digit year fits in 16 bits |
| Month month | `uint8_t` | 1 B | Values 1–12 |
| Floor area | `uint16_t` | 2 B | Max ~200 m² |
| Lease commence date | `uint16_t` | 2 B | 4-digit year |
| Resale price | `uint32_t` | 4 B | Up to ~$1.5M SGD |
| String columns | `std::string` | var | No encoding in baseline; Dict encoding adds uint16 encoded columns |

The source `Month` column (`"YYYY-MM"`) is **decomposed at ingestion** into two integer columns, eliminating repeated string parsing during scans.

---

## Requirements

- **C++17** or later
- **g++** (GCC) or **clang++** — tested on macOS (Apple Clang) and Linux (GCC 11+)
- **GNU Make**
- `ResalePricesSingapore.csv` placed in `../data/` relative to the `Project/` directory

---

## Building

```bash
# Build the executable
make

# Build and run with our matric number (U2220136J)
make run

# Build and run the full evaluation suite (50+ configs, 5 iterations each)
make eval

# Remove all build artefacts
make clean
```

To build manually without Make:

```bash
g++ -std=c++17 -Wall -Wextra -Iinclude \
    main.cpp src/column_store.cpp src/csv_parser.cpp \
    src/query_engine.cpp src/output_writer.cpp src/column_file_io.cpp \
    -o column_store
```

---

## Running

### Basic usage (no optimisations — baseline)

```bash
./column_store U2220136J
```

### With optimisation flags

```bash
./column_store <MatriculationNumber> [flags...]
```

Flags can be combined in any order. Example:

```bash
./column_store U2220136J --dict-encoding --reuse
```

The program will:

1. Derive query parameters from the matriculation number.
2. Load `ResalePricesSingapore.csv` into the column store (applying any storage-layer flags).
3. Run all 568 `(x, y)` queries.
4. Write valid results to `ScanResult_U2220136J.csv`.

---

## Optimisation Flags

Every optimisation is independently toggleable via a boolean flag. The table below lists all runtime flags accepted by `./column_store`:

### Encoding Layer

| Flag | Description |
|------|-------------|
| `--dict-encoding` | Dictionary-encode Town, Flat_Type, Flat_Model, Street_Name. Replaces string comparisons with `uint16_t` integer comparisons. |
| `--precompute-ppsm` | Pre-compute `Resale_Price / Floor_Area` at load time into a parallel `double` column. Eliminates per-query floating-point division. |

### Physical Layout

| Flag | Description |
|------|-------------|
| `--presort-storage` | Sort all columns by (Town, Year, Month) after ingestion. Enables partition metadata and is a prerequisite for Binary Search and RLE Sort. |
| `--columnar-files` | Load from pre-written binary `.col` files instead of CSV. Reduces load time from ~2.5s to ~25ms. Requires a prior `--write-columns` run. |
| `--town-partitioning` | Load only the target towns' subdirectories from partitioned column files. Requires a prior `--write-columns-partitioned` run with Pre-Sort enabled. |

### Indexing

| Flag | Description |
|------|-------------|
| `--zone-maps` | Build per-chunk (min, max) metadata for Year, Month, Floor_Area. Skips entire 1024-row chunks that cannot satisfy query predicates. |
| `--bitmap-index-town` | Build a per-row bitmap mask for Town membership. Eliminates per-row Town string/int comparisons at query time. |
| `--month-bsearch` | Use binary search on the linearised month key within each town partition. Requires `--presort-storage`. |
| `--rle-town` | Build run-length encoding metadata over the Town column. At query time, skip entire non-target runs instead of row-by-row checks. Most effective with `--presort-storage`. |

### Scan-Path Optimisations

| Flag | Description |
|------|-------------|
| `--reuse` | Build a cumulative min-PPSM table in a single O(N) pass. All 568 queries become O(1) lookups. **Headline optimisation: 749× query speedup.** |
| `--late-materialise` | Defer loading of display-only columns (Block, Flat_Model, Street_Name) until a query result is confirmed. |
| `--predicate-reorder` | Evaluate Town predicate before Year/Month. **Negative result: 0.06× — rejected.** |
| `--int-multiply` | Integer early-exit gate: skip floating-point PPSM computation when `price > 4725 × area`. |

### I/O Layer

| Flag | Description |
|------|-------------|
| `--mmap-io` | Use POSIX `mmap(2)` instead of `ifstream` for column file loading. Requires `--columnar-files`. |

### Utility Flags (Data Preparation)

| Flag | Description |
|------|-------------|
| `--write-columns` | Ingest CSV, then write binary `.col` files to `data/columns*/` and exit. Used to prepare files for `--columnar-files`. |
| `--write-columns-partitioned` | Same as above, but writes one subdirectory per town. Used to prepare files for `--town-partitioning`. Requires `--presort-storage`. |

---

## Recommended Configurations

These configurations represent the best-performing paths discovered through our evaluation:

### Fastest total time (mmap + result reuse)

```bash
# Preparation (run once):
./column_store U2220136J --dict-encoding --precompute-ppsm --presort-storage --write-columns

# Query (71.9 ms total):
./column_store U2220136J --dict-encoding --reuse --columnar-files --mmap-io
```

### Fastest query time (partition pruning + result reuse)

```bash
# Preparation (run once):
./column_store U2220136J --dict-encoding --precompute-ppsm --presort-storage --write-columns-partitioned

# Query (0.7 ms query, 73.1 ms total — 3,389× query speedup):
./column_store U2220136J --dict-encoding --reuse --town-partitioning
```

### Best without pre-written files (CSV-only, no preparation step)

```bash
# 749× query speedup, single command:
./column_store U2220136J --dict-encoding --reuse
```

### Alternative scan-elimination path (pre-sort + binary search)

```bash
# 193× query speedup via binary search instead of result reuse:
./column_store U2220136J --presort-storage --month-bsearch
```

### RLE town skipping path

```bash
# 10.5× standalone; 180× when combined with binary search:
./column_store U2220136J --presort-storage --rle-town --month-bsearch
```

---

## Evaluation Suite

The evaluation suite benchmarks 50+ optimisation configurations, verifies correctness (byte-identical output) against baseline, and produces a detailed performance report.

### Building and running

```bash
# Using Make (builds and runs automatically):
make eval

# Or manually:
g++ -std=c++17 -Wall -Wextra -Iinclude \
    eval/eval_suite.cpp src/column_store.cpp src/csv_parser.cpp \
    src/query_engine.cpp src/output_writer.cpp src/column_file_io.cpp \
    -o eval_runner

./eval_runner ../data/ResalePricesSingapore.csv U2220136J 5
```

### Usage

```bash
./eval_runner <path_to_csv> <MatriculationNumber> [num_runs=5] [output_file]
```

| Argument | Description |
|----------|-------------|
| `path_to_csv` | Path to `ResalePricesSingapore.csv` |
| `MatriculationNumber` | Matric number to derive query parameters |
| `num_runs` | Number of benchmark iterations per configuration (default: 5) |
| `output_file` | Custom output filename (default: `EvalResult_<Matric>.txt` in `results/`) |

### Output

The suite writes results to both the terminal and a file in `results/`. The report includes:

- **Performance table** with load time, query time, total time, speedup, rows scanned, town comparisons, memory usage, and correctness status for each configuration.
- **Delta analysis** comparing each configuration against baseline.
- **Dictionary encoding stats**, **zone map stats**, **bitmap index stats**, and **RLE stats** where applicable.
- **Correctness verification**: every configuration's output is compared row-by-row against baseline. Exit code `0` = all correct, `1` = at least one mismatch.

### Interpreting the results

The `Speedup` column reports **query-phase speedup**: `Baseline_Query_ms / Config_Query_ms`. For end-to-end comparison, compute `Baseline_Total_ms / Config_Total_ms`. The two can diverge significantly — for example, C1+C2 achieves 813× query speedup but only ~1.7× total speedup because load time dominates. Conversely, A9 (columnar files) has modest query speedup but dramatically reduces load time, yielding strong total speedup.

---

## Query Parameter Derivation

Given a matriculation number (e.g. `U2220136J`):

### Target Year

The **last digit** maps to a year:

| Digit | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
|-------|---|---|---|---|---|---|---|---|---|---|
| Year | 2020 | 2021 | 2022 | 2023 | 2024 | 2015 | 2016 | 2017 | 2018 | 2019 |

### Start Month

The **second-last digit** is the commencing month. `0` maps to month 10 (October).

### Town List

Every **unique digit** in the full matric number maps to a town:

| Digit | Town | Digit | Town |
|-------|------|-------|------|
| 0 | BEDOK | 5 | JURONG WEST |
| 1 | BUKIT PANJANG | 6 | PASIR RIS |
| 2 | CLEMENTI | 7 | TAMPINES |
| 3 | CHOA CHU KANG | 8 | WOODLANDS |
| 4 | HOUGANG | 9 | YISHUN |

**Our matric `U2220136J`:** last digit `6` → year **2016**, second-last `3` → start month **March**, unique digits `{0, 1, 2, 3, 6}` → towns **{BEDOK, BUKIT PANJANG, CLEMENTI, CHOA CHU KANG, PASIR RIS}**.

---

## Output Format

Results are written to `ScanResult_<MatricNum>.csv`. Only valid `(x, y)` pairs (minimum PPSM ≤ 4725) are included, ordered by ascending `x` then ascending `y`.

```
(x, y),Year,Month,Town,Block,Floor_Area,Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter
(1, 80),2016,03,CHOA CHU KANG,701,109,Model A,1995,3028
(1, 81),2016,03,CHOA CHU KANG,701,109,Model A,1995,3028
...
```

| Field | Format | Description |
|-------|--------|-------------|
| `(x, y)` | `(int, int)` | The query pair |
| `Year` | `YYYY` | Year of the matched record |
| `Month` | `MM` | Month, zero-padded (e.g. `03`) |
| `Town` | string | Town name |
| `Block` | string | Block identifier |
| `Floor_Area` | int | Floor area in m² |
| `Flat_Model` | string | e.g. Standard, Improved, Model A |
| `Lease_Commence_Date` | `YYYY` | Year the lease began |
| `Price_Per_Square_Meter` | int | Minimum PPSM, rounded to nearest integer |

---

## Error Handling

| Situation | Behaviour |
|-----------|-----------|
| Input CSV not found | Error message, exit code 1 |
| Input CSV is empty | Error message, exit code 1 |
| Row has wrong number of fields | Row skipped, warning to `stderr` |
| Mandatory numeric field is empty | Row skipped, warning to `stderr` |
| Field fails numeric conversion | Row skipped, warning to `stderr` |
| Output file cannot be opened | Error message, exit code 1 |
| No records match an `(x, y)` query | That pair is silently omitted from output |
| Matric number has fewer than 2 digits | Error message, exit code 1 |
| Column file missing (A9/E1) | Auto-generated from CSV on first run |
| `--month-bsearch` without `--presort-storage` | Falls back to linear scan (no error) |
| `--rle-town` without `--presort-storage` | Works correctly but with limited speedup; warning printed |