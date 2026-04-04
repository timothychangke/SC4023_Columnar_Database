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