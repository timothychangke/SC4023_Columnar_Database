

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "column_store.h"

// low level parsing utilities

// strips whitespace from front and back of string
std::string trim(const std::string& s);


void parseCSVLine(const std::string& line, std::vector<std::string>& fields);


void parseMonthField(const std::string& month_str,
                     uint16_t& year, uint8_t& month);

// main ingestion function


std::size_t loadCSV(const std::string& filepath, ColumnStore& db);