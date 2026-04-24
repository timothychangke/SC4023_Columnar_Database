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

std::size_t computeIOChunkRows(std::size_t memory_budget_bytes,
                               bool        dict_encoding,
                               bool        precomputed_ppsm);

std::size_t loadColumnFilesChunk(const std::string& dir,
                                 std::size_t        chunk_start,
                                 std::size_t        chunk_rows,
                                 ColumnStore&       db);

void loadColumnFilesMmap(const std::string& dir, ColumnStore& db);

// mmap-backed lazy materialisation: avoids per-call ifstream open.
std::string loadStringAtMmap(const std::string& filepath, std::size_t idx);
uint16_t    loadUint16AtMmap(const std::string& filepath, std::size_t idx);

void writeColumnFilesPartitioned(const ColumnStore& db, const std::string& base_dir);

void loadColumnFilesPartitioned(const std::string& base_dir,
                                const std::vector<std::string>& target_towns,
                                ColumnStore& db);