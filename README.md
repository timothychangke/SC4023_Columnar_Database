# CE/CZ4123/SC4023 — Big Data Management: Semester Group Project

A column-oriented in-memory database engine written in C++ for querying Singapore HDB resale flat transaction records (2015–2025).

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Building](#building)
- [Usage](#usage)
- [Evaluation Suite Usage](#evaluation-suite-usage)
- [How Query Parameters Are Derived](#how-query-parameters-are-derived)
- [Output Format](#output-format)
- [Error Handling](#error-handling)

---

## Overview

This program ingests the `ResalePricesSingapore.csv` dataset and answers the following query:

> For each valid `(x, y)` pair — where `x` is a window of months (1–8) and `y` is a minimum floor area in m² (80–150) — find the HDB resale record with the **minimum price per square metre** within a filtered set of towns and time window. A pair is considered valid if that minimum price per square metre is **at most 4725 SGD/m²**.

Query parameters (target year, start month, and towns) are derived from a matriculation number provided at runtime.

---

## Project Structure

```
project/
├── Makefile
├── main.cpp                   # Entry point — orchestrates all four phases
├── include/
│   ├── column_store.h         # ColumnStore struct definition
│   ├── csv_parser.h           # CSV ingestion declarations
│   ├── query_engine.h         # QueryResult struct + query function declarations
│   └── output_writer.h        # Output writer declaration
└── src/
    ├── column_store.cpp       # ColumnStore::size() and ::clear()
    ├── csv_parser.cpp         # CSV parsing and loadCSV() implementation
    ├── query_engine.cpp       # Query parameter derivation and scan logic
    └── output_writer.cpp      # writeResults() implementation
```

Each file has a single, clearly bounded responsibility so that future changes — e.g. adding compression, changing the input format, or adding new query types — are isolated to the relevant file.

---

## Architecture

### Column-Oriented Storage

Data is stored in a strict **column store**. There is no `Row` or `Record` object anywhere in the codebase. Instead, each attribute of the dataset lives in its own independent `std::vector`, all kept at equal length (parallel alignment):

```
col_month_year[i]          col_month_month[i]
col_town[i]                col_block[i]
col_street_name[i]         col_flat_type[i]
col_flat_model[i]          col_storey_range[i]
col_floor_area[i]          col_lease_commence_date[i]
col_resale_price[i]
```

Index `i` across every vector always refers to the same logical transaction record. This layout avoids loading irrelevant columns during a scan and is the defining property of a column store.

### Data Type Choices

| Column              | C++ Type      | Rationale                                   |
| ------------------- | ------------- | ------------------------------------------- |
| Month year          | `uint16_t`    | 4-digit year; 2 bytes vs 4 for `int`        |
| Month month         | `uint8_t`     | Value 1–12; only 1 byte needed              |
| Floor area          | `uint16_t`    | Area in m²; max ~200, well within 65,535    |
| Lease commence date | `uint16_t`    | 4-digit year; same reasoning as above       |
| Resale price        | `uint32_t`    | Up to ~$1.5M SGD; safe within the 4.29B max |
| String columns      | `std::string` | No encoding applied at this stage           |

The `Month` source column (`"YYYY-MM"`) is **decomposed at ingestion time** into two integer columns. This avoids repeated string parsing on every query scan.

---

## Requirements

- C++17 or later
- `g++` (GCC) or any compatible compiler
- `make`
- `ResalePricesSingapore.csv` placed in the same directory as the executable

---

## Building

```bash
# Build the executable
make

# Remove compiled objects and the executable
make clean
```

To build manually without `make`:

```bash
g++ -std=c++17 -O2 -I include \
    src/column_store.cpp \
    src/csv_parser.cpp \
    src/query_engine.cpp \
    src/output_writer.cpp \
    eval/eval_suite.cpp \
    src/column_file_io.cpp \
    -o eval_suite
```

To run unit tests:

```bash
make test
# Or
g++ -std=c++17 -Wall -Wextra -Iinclude \
        tests/test_suite.cpp \
        src/column_store.cpp \
        src/csv_parser.cpp \
        src/query_engine.cpp \
        src/output_writer.cpp \
        src/column_file_io.cpp \
        -o test_runner
```

To build the evaluation suite:

```bash
g++ -std=c++17 -Wall -Wextra -Iinclude  eval/eval_suite.cpp  src/column_store.cpp src/csv_parser.cpp  src/query_engine.cpp  src/output_writer.cpp  src/column_file_io.cpp  -o eval_runner
```

---

## Usage

```bash
./column_store <MatriculationNumber>
```

**Example:**

```bash
./column_store A5656567B
```

The program will:

1. Derive the target year, start month, and town list from the matriculation number.
2. Load `ResalePricesSingapore.csv` into the column store.
3. Run all 568 `(x, y)` queries (`x` ∈ [1,8], `y` ∈ [80,150]).
4. Write valid results to `ScanResult_A5656567B.csv`.

### Optimisation flags (including A5)

Common runtime flags:

- `--dict-encoding` (A1)
- `--presort-storage` (A2)
- `--rle-town` or `--rle` (A5)
- `--month-bsearch` (B3)
- `--zone-maps` (B1)
- `--precompute-ppsm` (A4)
- `--int-multiply` (C6)
- `--predicate-reorder` (C4)
- `--late-materialise` (C3)
- `--reuse` (C1/C2)
- `--columnar-files` (A9)

Example:

```bash
./column_store A5656567B --dict-encoding --presort-storage --rle-town --month-bsearch
```

### A5: Run-Length Encoding (Town)

When `--rle-town` is enabled, the engine builds run metadata over the final in-memory row order:

- `town_run_value[k]` / `town_run_value_encoded[k]`
- `town_run_start[k]`
- `town_run_length[k]`

At query time, the engine jumps directly to runs for requested towns and skips non-target runs. This is most effective with A2 because town values become long contiguous regions.

Complexity summary:

- Build: $O(N)$
- Space: $O(R)$ where $R$ is number of runs
- Town-filter stage: from row-level $O(N)$ checks to run-level pruning plus interval scan

Console output during a run looks like:

```
Matriculation number : A5656567B
Target year  : 2017
Start month  : 6
Target towns : JURONG WEST, PASIR RIS, TAMPINES
---------------------------------------------------
Data Ingestion Complete:
  File           : ResalePricesSingapore.csv
  Lines read     : 192135 (excl. header)
  Records loaded : 192135
  Records skipped: 0
---------------------------------------------------
Total records in column store: 192135
Output written to : ScanResult_A5656567B.csv
Valid (x,y) pairs : 47
Done.
```

---

## Optimisation Flags (CLI)

Run with one or more flags:

```bash
./column_store <MatriculationNumber> [flags...]
```

Common flags:

- `--dict-encoding` (A1)
- `--bitmap-index-town` (B2)
- `--presort-storage` (A2)
- `--month-bsearch` (B3)
- `--zone-maps` (B1)
- `--precompute-ppsm` (A4)
- `--int-multiply` (C6)
- `--predicate-reorder` (C4)
- `--late-materialise` (C3)
- `--reuse` (C1/C2)
- `--columnar-files` (A9)

---

## Evaluation Suite Usage

If you have built the eval suite using the commands above, run from the `Project` directory:

```bash
./eval_runner <path_to_csv> <MatriculationNumber> [num_runs=5] [output_file]
```

Note that `make eval` will build the eval suite and run it automatically for 5 runs.

Examples:

```bash
# default output file: /results/EvalResult_A5656567B.txt
./eval_runner ../data/ResalePricesSingapore.csv A5656567B 5

# custom output file name: /results/mycustom67file.txt
./eval_runner ../data/ResalePricesSingapore.csv A5656567B 5 mycustom67file.txt
```

Notes:

- The suite now writes the full report to file **and** prints progress to terminal.
- Exit code `0` means all configurations matched baseline correctness.
- Exit code `1` means at least one configuration failed correctness.

### How to interpret the evaluation results

```sh
Configuration                      Load (ms)    Query (ms)    Total (ms)       Speedup  Rows Scanned     Town Cmps     Rows Pass   Valid (x,y)        Memory
------------------------------------------------------------------------------------------------------------------------------------------------------------
Baseline                               762.3      1214.695        1977.0         1.00x       147.25M        12.55M        308.9K           568       73.2 MB
C1+C2: Result Reuse                    839.2         2.471         841.6       491.58x        259.2K         12.5K             0           568       73.2 MB
A9+A2+B3: ColFile+Presort+MonthBSearch          20.4       115.724         136.1        10.50x        751.7K             0        308.9K           568        2.7 MB
```

The `Speedup` column in the performance table is currently **query-phase speedup**:

$$
  ext{Query Speedup} = \frac{\text{Baseline Query Time}}{\text{Config Query Time}}
$$

This is useful, but it does **not** include data loading/setup cost.

For end-to-end comparison, also compute:

$$
  ext{Overall Speedup} = \frac{\text{Baseline Total Time}}{\text{Config Total Time}}
$$

#### Example 1: `C1+C2` (Result Reuse)

- Query speedup shown in table: **491.58x**
  - Baseline query: `1214.695 ms`
  - `C1+C2` query: `2.471 ms`
- Overall speedup (using total):
  - Baseline total: `1977.0 ms`
  - `C1+C2` total: `841.6 ms`
  - Overall: $1977.0 / 841.6 \approx 2.35\text{x}$

Interpretation: excellent query acceleration, but total gain is smaller because load/setup still costs time.

#### Example 2: `A9+A2+B3` (Column Files + Presort + Month Binary Search)

- Query speedup shown in table: **10.50x**
  - Baseline query: `1214.695 ms`
  - `A9+A2+B3` query: `115.724 ms`
- Overall speedup (using total):
  - Baseline total: `1977.0 ms`
  - `A9+A2+B3` total: `136.1 ms`
  - Overall: $1977.0 / 136.1 \approx 14.53\text{x}$

Interpretation: this strategy improves both load and query time, so overall speedup is very strong.

#### Most essential statistics to report

When comparing configurations, prioritize these columns:

1. **Total (ms)** : primary end-to-end metric.
2. **Load (ms)** and **Query (ms)** : explains where gains/losses come from.
3. **Rows Scanned** and **Town Cmps** : confirms pruning/filter efficiency.
4. **Memory** : checks space-performance tradeoff.
5. **Valid (x,y)** : must match baseline (correctness guard).

Practical rule: pick the configuration with the best **Total (ms)** among those with identical correctness.

---

## How Query Parameters Are Derived

Given a matriculation number (e.g. `A5656567B`), the program extracts the following:

### Target Year

The **last digit** of the matric number maps to a year:

| Last digit | 0    | 1    | 2    | 3    | 4    | 5    | 6    | 7    | 8    | 9    |
| ---------- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Year       | 2020 | 2021 | 2022 | 2023 | 2024 | 2015 | 2016 | 2017 | 2018 | 2019 |

> Note: 2025 is excluded as a target year per the project specification (it is used for querying only).

### Start Month

The **second-last digit** of the matric number is the commencing month. `0` maps to October (month 10).

### Town List

Every **unique digit** in the full matric number maps to a town via Table 1:

| Digit | Town          | Digit | Town        |
| ----- | ------------- | ----- | ----------- |
| 0     | BEDOK         | 5     | JURONG WEST |
| 1     | BUKIT PANJANG | 6     | PASIR RIS   |
| 2     | CLEMENTI      | 7     | TAMPINES    |
| 3     | CHOA CHU KANG | 8     | WOODLANDS   |
| 4     | HOUGANG       | 9     | YISHUN      |

**Example** — `A5656567B`:

- Last digit `7` → year **2017**
- Second-last digit `6` → start month **June (6)**
- Unique digits `{5, 6, 7}` → towns **JURONG WEST, PASIR RIS, TAMPINES**

---

## Output Format

Results are written to `ScanResult_<MatricNum>.csv`. Only valid `(x, y)` pairs (minimum price per sqm ≤ 4725) are included. Rows are ordered by ascending `x`, then ascending `y`.

```
(x, y),Year,Month,Town,Block,Floor_Area,Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter
(1, 80),2017,06,TAMPINES,274,105,New Generation,1985,3847
(1, 81),2017,06,TAMPINES,274,105,New Generation,1985,3847
...
```

| Field                    | Description                                          |
| ------------------------ | ---------------------------------------------------- |
| `(x, y)`                 | The query pair                                       |
| `Year`                   | Year of the matched record (`YYYY`)                  |
| `Month`                  | Month of the matched record (`MM`, zero-padded)      |
| `Town`                   | Town of the matched flat                             |
| `Block`                  | Block identifier                                     |
| `Floor_Area`             | Floor area in m²                                     |
| `Flat_Model`             | Flat model (e.g. Standard, Improved, New Generation) |
| `Lease_Commence_Date`    | Year the lease began                                 |
| `Price_Per_Square_Meter` | Minimum price per m², rounded to the nearest integer |

---

## Error Handling

| Situation                             | Behaviour                                          |
| ------------------------------------- | -------------------------------------------------- |
| Input file not found                  | Prints an error message and exits with code `1`    |
| Input file is empty                   | Prints an error message and exits with code `1`    |
| Row has wrong number of fields        | Row is skipped; warning printed to `stderr`        |
| Mandatory numeric field is empty      | Row is skipped; warning printed to `stderr`        |
| Field fails numeric conversion        | Row is skipped; warning printed to `stderr`        |
| Output file cannot be opened          | Prints an error message and exits with code `1`    |
| No records match an `(x, y)` query    | That `(x, y)` pair is silently omitted from output |
| Matric number has fewer than 2 digits | Prints an error message and exits with code `1`    |
