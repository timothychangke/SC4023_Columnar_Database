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
# For Windows:
Remove-Item -Force -ErrorAction SilentlyContinue main.o, src\column_store.o, src\csv_parser.o, src\query_engine.o, src\output_writer.o, column_store.exe, column_store ; make
```

To build manually without `make`:

```bash
g++ -std=c++17 -O2 -I include \
    src/column_store.cpp \
    src/csv_parser.cpp \
    src/query_engine.cpp \
    src/output_writer.cpp \
    eval/eval_suite.cpp \
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
        -o test_runner
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
