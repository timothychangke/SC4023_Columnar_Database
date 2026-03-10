/*
 * declares the query parameter extraction and the main query execution logic.
 *
 * what this does:
 * - parse the matric number to find target year, start month, and allowed towns
 * - run the x, y query directly against our ColumnStore
 *
 * Query spec recap:
 * for a given (x,y), find records where:
 * - YEAR == target_year
 * - MONTH is between start_month and start_month + x - 1
 * - Town is inside the allowed list
 * - Floor_Area >= y
 * then find the one with the lowest (Resale_Price / Floor_Area).
 * result is only valid if this minimum is <= 4725.
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "column_store.h"

// query result struct

/*
 * QueryResult
 * struct just to hold the final output fields for printing.
 * NOTE: dont use this to store data during the scan. we are building a
 * column store, so all computations must run directly on the vectors.
 */
struct QueryResult {
    int      x            = 0;
    int      y            = 0;
    uint16_t year         = 0;
    uint8_t  month        = 0;
    std::string town;
    std::string block;
    uint16_t floor_area          = 0;
    std::string flat_model;
    uint16_t lease_commence_date = 0;
    double   price_per_sqm       = 0.0; // Resale_Price / Floor_Area
    bool     no_result           = false; // true if nothing matches or min > 4725
};

// MinEntry
// struct to hold the minimum ppsm and corresponding record index for a given (x,y) 
// during the cumulative table building.
struct MinEntry { 
    bool has = false; 
    double ppsm = 0.0; 
    std::size_t idx = 0; 
};

// helpers to get query params

/*
 * buildTownList
 * get the list of towns based on the digits in the matric number.
 * uses table 1 from the project spec. duplicate digits just map to same town once.
 *
 * 0=BEDOK, 1=BUKIT PANJANG, 2=CLEMENTI, 3=CHOA CHU KANG, 4=HOUGANG
 * 5=JURONG WEST, 6=PASIR RIS, 7=TAMPINES, 8=WOODLANDS, 9=YISHUN
 *
 * @param matric_number full matric string eg "A1234567B"
 * @return vector of unique uppercase town strings
 */
std::vector<std::string> buildTownList(const std::string& matric_number);

/*
 * deriveQueryParams
 * extract base year and start month from matric number:
 * - target year: based on the LAST digit of the matric
 * - start month: based on the SECOND LAST digit (0 becomes Oct = 10)
 *
 * Year mapping (last digit to year, spec says 2025 excluded):
 * 0=2020, 1=2021, 2=2022, 3=2023, 4=2024
 * 5=2015, 6=2016, 7=2017, 8=2018, 9=2019
 *
 * @param matric_number full matric string
 * @param target_year outputs the 4 digit year
 * @param start_month outputs month 1 to 12
 * @throws std::invalid_argument if matric string got less than 2 digits
 */
void deriveQueryParams(const std::string& matric_number,
                       uint16_t& target_year, uint8_t& start_month);

// core execution

/*
 * runQuery
 * run the actual (x,y) query by scanning the ColumnStore.
 *
 * applies 4 filters column by column:
 * 1. col_month_year == target_year
 * 2. col_month_month in [start_month, start_month + x - 1] (capped at 12)
 * 3. col_town is in the towns list
 * 4. col_floor_area >= y
 *
 * out of all records that pass, find the one with lowest Resale_Price/Floor_Area.
 * only record it if this minimum is <= 4725.
 *
 * @param db the populated ColumnStore
 * @param x query duration in months (1 to 8)
 * @param y min floor area (80 to 150)
 * @param target_year year to filter
 * @param start_month start of month range
 * @param towns allowed towns
 * @param result output struct to populate
 */
void runQuery(const ColumnStore&              db,
              int                             x,
              int                             y,
              uint16_t                        target_year,
              uint8_t                         start_month,
              const std::vector<std::string>& towns,
              QueryResult&                    result,
              const std::vector<std::vector<MinEntry>>& cum_table);

/*
 * buildCumulativeTable
 * Preprocessing step for intermediate result optimisation.
 * Previously, all rows of the table were scanned for each (x,y) query.
 * - A x=3 month window contains records from x=2 and x=1.
 *   Instead of scanning for x=1, x=2, x=3 separately, we scan once and
 *   label each record with which month offset it belongs to (1..8).
 * - A y=80 floor area threshold contains records satisfying y=81, y=82, ...
 *   We group records by their exact area bucket (80..150) once.
 * Then we build a cumulative table cum_x in two sweeps:
 * - X sweep (offsets 1->8): cum_x[x][area] = min PPSM over offsets 1..x
 * - Y sweep (area 149->80): propagate min downward so that
 *   cum_x[x][y] = min PPSM for any record with month_offset <= x AND floor_area >= y.
 * Result: O(N) table build, O(1) lookup for any (x,y) pair.
 */
std::vector<std::vector<MinEntry>> buildCumulativeTable(
    const ColumnStore&              db,
    uint16_t                        target_year,
    uint8_t                         start_month,
    const std::vector<std::string>& towns
);