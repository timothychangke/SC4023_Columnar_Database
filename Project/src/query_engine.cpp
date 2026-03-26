/*
 * implementation for the parameter extraction and columnar scan.
 *
 * === OPTIMISATION: Dictionary Encoding  ===
 * When db.use_dict_encoding is true, runQuery converts the town filter list
 * into encoded integer IDs once, then uses int==int comparison in the hot loop
 * instead of string==string. This avoids heap-allocated string comparisons
 * on every row, giving a measurable speedup on the town filter predicate.
 */

#include "query_engine.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <string>
#include <vector>
#include <sstream>
#include <unordered_set>

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
    int last_digit        = -1;
    int second_last_digit = -1;
    int digits_found      = 0;

    for (int i = static_cast<int>(matric_number.size()) - 1; i >= 0; --i) {
        char c = matric_number[i];
        if (c >= '0' && c <= '9') {
            ++digits_found;
            if (digits_found == 1) last_digit        = c - '0';
            if (digits_found == 2) second_last_digit = c - '0';
            if (digits_found == 2) break;
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
    result.no_result = true;

    // cap end month at 12 so we dont accidentally query into the next year.
    const uint8_t end_month = static_cast<uint8_t>(
        std::min(static_cast<int>(start_month) + x - 1, 12));

    double      min_ppsm = std::numeric_limits<double>::max();
    std::size_t best_i   = 0;

    const std::size_t N = db.size();

    // === OPTIMISATION 1: Intermediate Result Reuse path ===
    if (db.use_reuse) {
        // answer this (x,y) query directly from the pre-built cumulative table
        const MinEntry &e = db.cum_table[x][y];

        if (e.has) {
            best_i = e.idx;
            min_ppsm = e.ppsm;
            result.no_result = false;
        }
        
    } else if (db.use_dict_encoding) {
        // === OPTIMISATION 2: Dictionary Encoding path ===
        // when dict encoding is on, pre-resolve town strings to integer IDs
        // so the loop compares uint16_t instead of std::string.

        // pre-resolve town names to their encoded IDs.
        // if a town isn't in the dictionary at all, it means zero records
        // have that town, so we can skip it.
        std::vector<uint16_t> town_ids;
        town_ids.reserve(towns.size());
        for (const auto& t : towns) {
            uint16_t id;
            if (db.dict_town.lookup(t, id)) {
                town_ids.push_back(id);
            }
            // if lookup fails, that town has no records so skip it silently
        }

        // if none of the requested towns exist in the data, no results possible
        if (town_ids.empty()) return;

        // loop with integer comparisons
        for (std::size_t i = 0; i < N; ++i) {

            // filter 1: year match
            if (db.col_month_year[i] != target_year) continue;

            // filter 2: month in range
            const uint8_t m = db.col_month_month[i];
            if (m < start_month || m > end_month) continue;

            // filter 3: town match (int==int comparison, much faster)
            bool town_match = false;
            const uint16_t row_town_id = db.col_town_encoded[i];
            for (const auto& tid : town_ids) {
                if (row_town_id == tid) { town_match = true; break; }
            }
            if (!town_match) continue;

            // filter 4: floor area threshold
            if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;

            // record survived all filters. calculate price per sqm.
            const double ppsm =
                static_cast<double>(db.col_resale_price[i]) /
                static_cast<double>(db.col_floor_area[i]);

            if (ppsm < min_ppsm) {
                min_ppsm = ppsm;
                best_i   = i;
                result.no_result = false;
            }
        }
    } else {
        // === BASELINE path ===
        // uses original string comparisons, exactly as before.
        for (std::size_t i = 0; i < N; ++i) {

            // filter 1: year match
            if (db.col_month_year[i] != target_year) continue;

            // filter 2: month in range
            const uint8_t m = db.col_month_month[i];
            if (m < start_month || m > end_month) continue;

            // filter 3: town match
            bool town_match = false;
            for (const auto& t : towns) {
                if (db.col_town[i] == t) { town_match = true; break; }
            }
            if (!town_match) continue;

            // filter 4: floor area threshold
            if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;

            // record survived all filters. calculate price per sqm.
            const double ppsm =
                static_cast<double>(db.col_resale_price[i]) /
                static_cast<double>(db.col_floor_area[i]);

            if (ppsm < min_ppsm) {
                min_ppsm = ppsm;
                best_i   = i;
                result.no_result = false;
            }
        }
    }

    // Return early if no results
    if (result.no_result) {
        return;
    }

    // Return early if min PPSM is too high
    if (min_ppsm > 4725.0) {
        result.no_result = true;
        return;
    }

    // Populate the final result from the column vectors
    // - Always use the original string columns for output (never decode).
    result.year                = db.col_month_year[best_i];
    result.month               = db.col_month_month[best_i];
    result.town                = db.col_town[best_i];
    result.block               = db.col_block[best_i];
    result.floor_area          = db.col_floor_area[best_i];
    result.flat_model          = db.col_flat_model[best_i];
    result.lease_commence_date = db.col_lease_commence_date[best_i];
    result.price_per_sqm       = min_ppsm;
}

// Preprocessing step for intermediate result optimisation
std::vector<std::vector<MinEntry>> buildCumulativeTable(
                const ColumnStore&              db,
                uint16_t                        target_year,
                uint8_t                         start_month,
                const std::vector<std::string>& towns
) {
    const std::size_t N = db.size();

    // Initialize a 2D table, holding the per-offset (1..8) by area (0..150)
    std::vector<std::vector<MinEntry>> per_x(9, std::vector<MinEntry>(151));

    // Create a set to quickly check if a town is in the filter list
    std::unordered_set<std::string> town_set;
    std::vector<uint16_t> town_ids;

    if (db.use_dict_encoding) {
        for (const auto& t : towns) {
            uint16_t id;
            if (db.dict_town.lookup(t, id)) {
                town_ids.push_back(id);
            }
        }
    } else {
        town_set.insert(towns.begin(), towns.end());
    }

    // For each row in the table
    for (std::size_t i = 0; i < N; ++i) {
        // filter 1: year match
        if (db.col_month_year[i] != target_year) continue;

        // filter 2: month in range
        const uint8_t month = db.col_month_month[i];
        if (month < start_month) continue;
        // compute offset relative to start_month: 1..8
        int offset = static_cast<int>(month) - static_cast<int>(start_month) + 1;
        if (offset < 1 || offset > 8) continue;

        // filter 3: town match
        if (db.use_dict_encoding) {
            bool match = false;
            const uint16_t row_id = db.col_town_encoded[i];
            for (const auto& tid : town_ids) {
                if (row_id == tid) { match = true; break; }
            }
            if (!match) continue;
        } else {
            if (town_set.find(db.col_town[i]) == town_set.end()) continue;
        }

        // filter 4: floor area threshold
        // - Records with area > 150 still satisfy floor_area >= y
        //   for every y in [80,150], so clamp them into bucket 150 rather than
        //   discarding them.  
        // - Records with area < 80 can never satisfy any valid
        //   y threshold, so those are the only ones we truly skip.
        const unsigned area = db.col_floor_area[i];
        if (area < 80) continue;
        const unsigned bucket = (area > 150) ? 150u : area;

        // compute ppsm
        const double ppsm = static_cast<double>(db.col_resale_price[i]) /
                            static_cast<double>(db.col_floor_area[i]);

        // update the per-offset, per-area entry with min
        MinEntry &entry = per_x[offset][bucket];
        if (!entry.has || ppsm < entry.ppsm) {
            entry.has = true;
            entry.ppsm = ppsm;
            entry.idx  = i;
        }
    }

    // Sweep: cum_x[x][area] = min ppsm over offsets 1..x for records with floor_area == area. 
    // The C2 sweep below turns this into floor_area >= area.
    std::vector<std::vector<MinEntry>> cum_x(9, std::vector<MinEntry>(151));
    for (int area = 80; area <= 150; ++area) {
        MinEntry running;
        for (int off = 1; off <= 8; ++off) {
            MinEntry &cur = per_x[off][area];
            if (cur.has) {
                if (!running.has || cur.ppsm < running.ppsm) running = cur;
            }
            cum_x[off][area] = running;
        }
    }

    // Sweep: propagate min from high area down to low area so that
    //    cum_x[x][y] = min ppsm over offsets 1..x AND floor_area >= y.
    // We sweep area from 149 down to 80; bucket 150 is already holding its minimum so it is correct
    for (int off = 1; off <= 8; ++off) {
        for (int area = 149; area >= 80; --area) {
            const MinEntry &hi = cum_x[off][area + 1];
            MinEntry       &lo = cum_x[off][area];
            if (hi.has && (!lo.has || hi.ppsm < lo.ppsm)) {
                lo = hi;
            }
        }
    }

    return cum_x;
}