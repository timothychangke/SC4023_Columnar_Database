/*
 * declares the csv parsing functions and the main load file function.
 *
 * the actual csv format from data.gov.sg looks something like this:
 * month,town,flat_type,block,street_name,storey_range,floor_area_sqm,flat_model,lease_commence_date,resale_price
 *
 * Error handling plan:
 * - file cannot find -> throw std::runtime_error straight away
 * - bad row format -> skip the row, print warning to stderr so we know
 * - empty lines -> silently skip them
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "column_store.h"

// low level parsing utilities

// strips whitespace from front and back of string
std::string trim(const std::string& s);

/*
 * parseCSVLine
 * split a single csv line into a vector of strings.
 * [Image of finite state machine for parsing CSV with quotes]
 * handles quoted fields properly so commas inside the address string 
 * dont screw up the splitting.
 *
 * @param line raw line from csv
 * @param fields output vector (will be cleared and refilled)
 */
void parseCSVLine(const std::string& line, std::vector<std::string>& fields);

/*
 * parseMonthField
 * parse the month string "MMM-YY" (eg "Jan-15") into year and month int.
 * since dataset only covers 2015 to 2025, we just hardcode prepend "20" to the year.
 *
 * @param month_str raw string eg "Jan-15"
 * @param year output 4 digit year
 * @param month output month 1 to 12
 * @throws std::invalid_argument if format is rubbish
 */
void parseMonthField(const std::string& month_str,
                     uint16_t& year, uint8_t& month);

// main ingestion function

/*
 * loadCSV
 * read the whole ResalePricesSingapore.csv and dump into our db.
 * header row is skipped automatically.
 *
 * @param filepath path to csv
 * @param db the ColumnStore to dump data into
 * @return how many records successfully loaded
 * @throws std::runtime_error if file cannot open
 */
std::size_t loadCSV(const std::string& filepath, ColumnStore& db);