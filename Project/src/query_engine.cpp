/*
 * implementation for the parameter extraction and columnar scan.
 */

#include "query_engine.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

// helpers to get query params

std::vector<std::string> buildTownList(const std::string& matric_number) {
    // table 1 from spec: map digit to town
    static const std::string TOWN_MAP[10] = {
        "BEDOK",          // 0
        "BUKIT PANJANG",  // 1
        "CLEMENTI",       // 2
        "CHOA CHU KANG",  // 3
        "HOUGANG",        // 4
        "JURONG WEST",    // 5
        "PASIR RIS",      // 6
        "TAMPINES",       // 7
        "WOODLANDS",      // 8
        "YISHUN"          // 9
    };

    std::vector<std::string> towns;
    bool seen[10] = {false};

    for (char c : matric_number) {
        if (c >= '0' && c <= '9') {
            int digit = c - '0';
            if (!seen[digit]) {
                seen[digit] = true;
                towns.push_back(TOWN_MAP[digit]);
            }
        }
    }

    return towns;
}

void deriveQueryParams(const std::string& matric_number,
                       uint16_t& target_year, uint8_t& start_month) {
    // project spec:
    // target year = last digit of matric
    // start month = second last digit 
    //
    // note: we need to find the actual digits. standard NTU matric usually ends 
    // with a letter (eg U1234567A). so we scan from right to left, skip the 
    // letters until we hit the 1st and 2nd digits.
    //
    // eg "A2345678B"
    // scan right to left: skip 'B', '8' is last digit, '7' is second last.
    // result: year maps to 8 (2018), month maps to 7 (July)

    int last_digit        = -1;
    int second_last_digit = -1;
    int digits_found      = 0;

    for (int i = static_cast<int>(matric_number.size()) - 1; i >= 0; --i) {
        char c = matric_number[i];
        if (c >= '0' && c <= '9') {
            ++digits_found;
            if (digits_found == 1) last_digit        = c - '0';
            if (digits_found == 2) second_last_digit = c - '0';
            if (digits_found == 2) break; // got both already, stop loop
        }
    }

    if (digits_found < 2) {
        throw std::invalid_argument(
            "Matriculation number must contain at least 2 digit characters.");
    }

    // map last digit to year (2025 is excluded per spec)
    static const uint16_t YEAR_MAP[10] = {
        2020, // 0
        2021, // 1
        2022, // 2
        2023, // 3
        2024, // 4
        2015, // 5
        2016, // 6
        2017, // 7
        2018, // 8
        2019  // 9
    };
    target_year = YEAR_MAP[last_digit];

    // second last digit to month (0 becomes October)
    start_month = (second_last_digit == 0)
                      ? 10
                      : static_cast<uint8_t>(second_last_digit);
}

// core query execution

void runQuery(const ColumnStore&              db,
              int                             x,
              int                             y,
              uint16_t                        target_year,
              uint8_t                         start_month,
              const std::vector<std::string>& towns,
              QueryResult&                    result) {

    result.x         = x;
    result.y         = y;
    result.no_result = true; // assume fail first until we find a valid one

    // cap end month at 12 so we dont accidentally query into the next year.
    // spec says we must remain within the target year.
    const uint8_t end_month = static_cast<uint8_t>(
        std::min(static_cast<int>(start_month) + x - 1, 12));

    double      min_ppsm = std::numeric_limits<double>::max();
    std::size_t best_i   = 0;

    const std::size_t N = db.size();

    // columnar scan loop
    // [Image of columnar database query execution]
    // we apply predicates independently. use early continue to skip bad records fast.
    // notice we dont instantiate any Row objects here to save memory overhead.
    for (std::size_t i = 0; i < N; ++i) {

        // filter 1: year match
        if (db.col_month_year[i] != target_year) continue;

        // filter 2: month in range
        const uint8_t m = db.col_month_month[i];
        if (m < start_month || m > end_month) continue;

        // filter 3: town match
        // list is tiny (max 10) so just linear scan it. dont need bother with index.
        bool town_match = false;
        for (const auto& t : towns) {
            if (db.col_town[i] == t) { town_match = true; break; }
        }
        if (!town_match) continue;

        // filter 4: floor area threshold
        if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;

        // record survived all filters. calculate price per sqm.
        // use double to prevent weird integer truncation bugs when comparing close values.
        const double ppsm =
            static_cast<double>(db.col_resale_price[i]) /
            static_cast<double>(db.col_floor_area[i]);

        if (ppsm < min_ppsm) {
            min_ppsm = ppsm;
            best_i   = i;
            result.no_result = false;
        }
    }

    // post scan validation
    // check if the best valid pair is actually below the 4725 spec limit.
    if (!result.no_result) {
        if (min_ppsm > 4725.0) {
            result.no_result = true;
            return;
        }

        // limit passed. populate the final result from the column vectors.
        result.year                = db.col_month_year[best_i];
        result.month               = db.col_month_month[best_i];
        result.town                = db.col_town[best_i];
        result.block               = db.col_block[best_i];
        result.floor_area          = db.col_floor_area[best_i];
        result.flat_model          = db.col_flat_model[best_i];
        result.lease_commence_date = db.col_lease_commence_date[best_i];
        result.price_per_sqm       = min_ppsm;
    }
}