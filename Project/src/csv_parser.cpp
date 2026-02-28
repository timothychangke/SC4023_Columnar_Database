/*
 * implementations for csv parsing and ingestion.
 *
 * NOTE for whoever takes over this:
 * The data.gov.sg dataset actually has 11 columns now, not 10.
 * They added "remaining_lease" at index 9 recently. We DONT need it for our query.
 *
 * We used to have this stupid bug where it always output "568 valid pairs".
 * Root cause was we hardcoded the index, so resale_price read from col 9 instead of 10.
 * It read the remaining lease (eg "61") as price. 
 * Then ppsm became 61 / 100sqm = 0.61. 
 * Since 0.61 is below the threshold, every single pair looked valid. 
 * * So now we parse the header dynamically to find the correct column index.
 * Dont hardcode the column positions anymore la.
 */

#include "csv_parser.h"

#include <array>
#include <climits>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>

// low level parsing utilities

std::string trim(const std::string& s) {
    const std::string WHITESPACE = " \t\r\n";
    std::size_t start = s.find_first_not_of(WHITESPACE);
    if (start == std::string::npos) return "";
    std::size_t end = s.find_last_not_of(WHITESPACE);
    return s.substr(start, end - start + 1);
}

void parseCSVLine(const std::string& line, std::vector<std::string>& fields) {
    fields.clear();
    std::string current_field;
    bool inside_quotes = false;

    for (std::size_t i = 0; i < line.size(); ++i) {
        char c = line[i];

        if (inside_quotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    current_field += '"';
                    ++i;
                } else {
                    inside_quotes = false;
                }
            } else {
                current_field += c;
            }
        } else {
            if (c == '"') {
                inside_quotes = true;
            } else if (c == ',') {
                fields.push_back(trim(current_field));
                current_field.clear();
            } else {
                current_field += c;
            }
        }
    }

    fields.push_back(trim(current_field));
}

void parseMonthField(const std::string& month_str,
                     uint16_t& year, uint8_t& month) {
    // expect "MMM-YY" eg "Jan-15"
    if (month_str.size() < 6 || month_str[3] != '-') {
        throw std::invalid_argument(
            "Invalid Month format (expected MMM-YY): '" + month_str + "'");
    }

    static const std::array<const char*, 12> MONTH_NAMES = {{
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec"
    }};

    char mon_lower[4] = {
        static_cast<char>(std::tolower(static_cast<unsigned char>(month_str[0]))),
        static_cast<char>(std::tolower(static_cast<unsigned char>(month_str[1]))),
        static_cast<char>(std::tolower(static_cast<unsigned char>(month_str[2]))),
        '\0'
    };

    month = 0;
    for (int i = 0; i < 12; ++i) {
        if (std::strcmp(mon_lower, MONTH_NAMES[i]) == 0) {
            month = static_cast<uint8_t>(i + 1);
            break;
        }
    }
    if (month == 0) {
        throw std::invalid_argument(
            "Unrecognised month abbreviation: '" + month_str.substr(0, 3) + "'");
    }

    try {
        int two_digit_year = std::stoi(month_str.substr(4));
        year = static_cast<uint16_t>(2000 + two_digit_year);
    } catch (const std::exception& e) {
        throw std::invalid_argument(
            "Failed to parse year from: '" + month_str + "' -- " + e.what());
    }

    if (year < 2015 || year > 2025) {
        throw std::invalid_argument(
            "Year " + std::to_string(year) +
            " is outside expected range [2015, 2025]");
    }
}

// main ingestion function

std::size_t loadCSV(const std::string& filepath, ColumnStore& db) {

    std::ifstream infile(filepath);
    if (!infile.is_open()) {
        throw std::runtime_error(
            "Cannot open input file '" + filepath + "'. "
            "Ensure the file exists in the working directory.");
    }

    // read header row to figure out column index dynamically.
    // doing this so if data.gov.sg add more columns next time we dont break again.
    std::string line;
    if (!std::getline(infile, line)) {
        throw std::runtime_error(
            "Input file '" + filepath + "' appears to be empty.");
    }

    std::vector<std::string> header_fields;
    parseCSVLine(line, header_fields);

    // map header names to index
    int COL_MONTH  = -1, COL_TOWN   = -1, COL_FLAT_TYPE = -1;
    int COL_BLOCK  = -1, COL_STREET = -1, COL_STOREY    = -1;
    int COL_AREA   = -1, COL_MODEL  = -1, COL_LEASE     = -1;
    int COL_PRICE  = -1;

    for (int i = 0; i < static_cast<int>(header_fields.size()); ++i) {
        const std::string& h = header_fields[i];
        if      (h == "month")                COL_MONTH     = i;
        else if (h == "town")                 COL_TOWN      = i;
        else if (h == "flat_type")            COL_FLAT_TYPE = i;
        else if (h == "block")                COL_BLOCK     = i;
        else if (h == "street_name")          COL_STREET    = i;
        else if (h == "storey_range")         COL_STOREY    = i;
        else if (h == "floor_area_sqm")       COL_AREA      = i;
        else if (h == "flat_model")           COL_MODEL     = i;
        else if (h == "lease_commence_date")  COL_LEASE     = i;
        else if (h == "resale_price")         COL_PRICE     = i;
        // ignore remaining_lease on purpose
    }

    // make sure got all required columns
    const bool all_found = (COL_MONTH >= 0 && COL_TOWN >= 0 && COL_FLAT_TYPE >= 0 &&
                            COL_BLOCK >= 0 && COL_STREET >= 0 && COL_STOREY >= 0 &&
                            COL_AREA  >= 0 && COL_MODEL  >= 0 && COL_LEASE   >= 0 &&
                            COL_PRICE >= 0);
    if (!all_found) {
        throw std::runtime_error(
            "CSV header is missing one or more required columns. "
            "Check that the file is ResalePricesSingapore.csv.");
    }

    const int EXPECTED_FIELDS = static_cast<int>(header_fields.size());

    std::cout << "Detected " << EXPECTED_FIELDS << " columns in CSV header.\n";
    std::cout << "Column positions: month=" << COL_MONTH
              << " town=" << COL_TOWN
              << " area=" << COL_AREA
              << " price=" << COL_PRICE << "\n";

    // start reading data rows
    std::vector<std::string> fields;
    fields.reserve(EXPECTED_FIELDS);

    std::size_t line_number     = 1;
    std::size_t records_loaded  = 0;
    std::size_t records_skipped = 0;

    while (std::getline(infile, line)) {
        ++line_number;

        if (trim(line).empty()) continue;

        parseCSVLine(line, fields);

        if (static_cast<int>(fields.size()) != EXPECTED_FIELDS) {
            std::cerr << "Warning [Line " << line_number << "]: Expected "
                      << EXPECTED_FIELDS << " fields, got " << fields.size()
                      << ". Skipping.\n";
            ++records_skipped;
            continue;
        }

        if (fields[COL_MONTH].empty() || fields[COL_AREA].empty() ||
            fields[COL_LEASE].empty() || fields[COL_PRICE].empty()) {
            std::cerr << "Warning [Line " << line_number << "]: "
                      << "One or more mandatory fields are empty. Skipping.\n";
            ++records_skipped;
            continue;
        }

        try {
            uint16_t rec_year  = 0;
            uint8_t  rec_month = 0;
            parseMonthField(fields[COL_MONTH], rec_year, rec_month);

            float raw_area = std::stof(fields[COL_AREA]);
            if (raw_area < 0.0f || raw_area > 65535.0f) {
                throw std::invalid_argument(
                    "floor_area_sqm out of range: " + fields[COL_AREA]);
            }
            uint16_t rec_floor_area = static_cast<uint16_t>(raw_area);

            int raw_lcd = std::stoi(fields[COL_LEASE]);
            if (raw_lcd < 1900 || raw_lcd > 2100) {
                throw std::invalid_argument(
                    "lease_commence_date out of plausible range: " +
                    fields[COL_LEASE]);
            }
            uint16_t rec_lease = static_cast<uint16_t>(raw_lcd);

            long long raw_price = std::stoll(fields[COL_PRICE]);
            if (raw_price < 0 ||
                raw_price > static_cast<long long>(UINT32_MAX)) {
                throw std::invalid_argument(
                    "resale_price out of uint32_t range: " + fields[COL_PRICE]);
            }
            uint32_t rec_price = static_cast<uint32_t>(raw_price);

            db.col_month_year.push_back(rec_year);
            db.col_month_month.push_back(rec_month);
            db.col_town.push_back(fields[COL_TOWN]);
            db.col_block.push_back(fields[COL_BLOCK]);
            db.col_street_name.push_back(fields[COL_STREET]);
            db.col_flat_type.push_back(fields[COL_FLAT_TYPE]);
            db.col_flat_model.push_back(fields[COL_MODEL]);
            db.col_storey_range.push_back(fields[COL_STOREY]);
            db.col_floor_area.push_back(rec_floor_area);
            db.col_lease_commence_date.push_back(rec_lease);
            db.col_resale_price.push_back(rec_price);

            ++records_loaded;

        } catch (const std::exception& e) {
            std::cerr << "Warning [Line " << line_number << "]: Parse error -- "
                      << e.what() << ". Skipping.\n";
            ++records_skipped;
        }
    }

    infile.close();

    std::cout << "---------------------------------------------------\n";
    std::cout << "Data Ingestion Complete:\n";
    std::cout << "  File           : " << filepath << "\n";
    std::cout << "  Lines read     : " << (line_number - 1) << " (excl. header)\n";
    std::cout << "  Records loaded : " << records_loaded  << "\n";
    std::cout << "  Records skipped: " << records_skipped << "\n";
    std::cout << "---------------------------------------------------\n";

    return records_loaded;
}