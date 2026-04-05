#pragma once
#include "column_store.h"
#include <string>
#include <cstdint>

// Write all columns from a populated ColumnStore to separate binary files.
void writeColumnFiles(const ColumnStore& db, const std::string& dir);

// Load only the filter columns needed for query scanning from binary files.
void loadColumnFiles(const std::string& dir, ColumnStore& db);

// Lazy materialisation: load a single string at row index from a string column file.
std::string loadStringAt(const std::string& filepath, std::size_t idx);

// Lazy materialisation: load a single uint16_t at row index from a numeric column file.
uint16_t loadUint16At(const std::string& filepath, std::size_t idx);

// D1: compute how many rows fit into one I/O chunk given a memory budget.
// Takes relevant flags into account because dict encoding and precomputed
// PPSM change the per-row cost of filter columns. Returns at least 1024.
std::size_t computeIOChunkRows(std::size_t memory_budget_bytes,
                               bool        dict_encoding,
                               bool        precomputed_ppsm);

// D1: load ONE contiguous row range of filter columns from the .col files
// into `db`. Only the columns the scan path reads are populated:
//   col_month_year, col_month_month, col_floor_area, col_resale_price,
//   col_town_encoded (dict_encoding required), and optionally
//   col_price_per_sqm (if precomputed_ppsm).
// `db` is assumed to already be clear()ed by the caller.
// Increments db.io_bytes_read. Returns the number of rows actually loaded
// (may be less than requested on the final chunk).
std::size_t loadColumnFilesChunk(const std::string& dir,
                                 std::size_t        chunk_start,
                                 std::size_t        chunk_rows,
                                 ColumnStore&       db);