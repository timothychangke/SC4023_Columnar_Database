/*
 * implementation for the parameter extraction and columnar scan.
 *
 * === ARCHITECTURE: Composable Optimisation Flags ===
 * Instead of separate code paths per optimisation, runQuery uses a SINGLE
 * scan loop where each flag controls an isolated decision point:
 *
 *   use_dict_encoding    (A1) — town comparison uses int==int vs string==string
 *   use_predicate_reorder(C4) — Town filter moves before Year/Month
 *   use_int_multiply     (C6) — integer early-exit gate before PPSM calc
 *   use_precomputed_ppsm (A4) — read pre-computed PPSM vs. divide on the fly
 *   use_reuse            (C1/C2) — bypasses the scan entirely (O(1) lookup)
 *
 * Any combination of flags just works. The reuse path is the only one that
 * short-circuits with an early return since it skips the scan altogether.
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

// ============================================================================
// Helper functions (unchanged)
// ============================================================================

std::vector<std::string> buildTownList(const std::string& matric_number) {
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
        2020, 2021, 2022, 2023, 2024,
        2015, 2016, 2017, 2018, 2019
    };
    target_year = YEAR_MAP[last_digit];

    // second last digit to month (0 becomes October)
    start_month = (second_last_digit == 0)
                      ? 10
                      : static_cast<uint8_t>(second_last_digit);
}

// ============================================================================
// Core query execution — unified scan loop
// ============================================================================

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

    // === REUSE PATH: O(1) table lookup, bypasses scan entirely ===
    // This is the only path that short-circuits. If the cumulative table
    // has been built, we just read the answer directly. All other flags
    // are irrelevant here since there is no scan loop to optimise.
    if (db.use_reuse && !db.cum_table.empty()) {
        const MinEntry &e = db.cum_table[x][y];

        if (!e.has) return;

        // populate result from the stored index
        result.no_result       = false;
        result.year            = db.col_month_year[e.idx];
        result.month           = db.col_month_month[e.idx];
        result.town            = db.col_town[e.idx];
        result.block           = db.col_block[e.idx];
        result.floor_area      = db.col_floor_area[e.idx];
        result.flat_model      = db.col_flat_model[e.idx];
        result.lease_commence_date = db.col_lease_commence_date[e.idx];
        result.price_per_sqm   = e.ppsm;

        // still need to enforce the 4725 threshold
        if (e.ppsm > 4725.0) {
            result.no_result = true;
        }
        return;
    }

    // === SCAN PATH: single loop, all flags compose inside ===

    const uint8_t end_month = static_cast<uint8_t>(
        std::min(static_cast<int>(start_month) + x - 1, 12));

    double      min_ppsm = std::numeric_limits<double>::max();
    std::size_t best_i   = 0;
    const std::size_t N  = db.size();

    // --- Pre-resolve town IDs once if dict-encoding is on (A1) ---
    // Done outside the loop so we pay the lookup cost only once per query,
    // not once per row.
    std::vector<uint16_t> town_ids;
    if (db.use_dict_encoding) {
        town_ids.reserve(towns.size());
        for (const auto& t : towns) {
            uint16_t id;
            if (db.dict_town.lookup(t, id)) {
                town_ids.push_back(id);
            }
        }
        // if none of the requested towns exist in the data, no results possible
        if (town_ids.empty()) return;
    }

    for (std::size_t i = 0; i < N; ++i) {

        // ==============================================================
        // PREDICATE BLOCK
        // C4 controls the ORDER of predicates.
        // A1 controls HOW the town predicate is evaluated.
        // These two flags are orthogonal — every combination works.
        // ==============================================================

        if (db.use_predicate_reorder) {
            // --- C4 ON: Town first (eliminates ~80% of rows) ---

            // town match (A1 controls int vs string comparison)
            if (db.use_dict_encoding) {
                bool match = false;
                const uint16_t row_id = db.col_town_encoded[i];
                for (const auto& tid : town_ids) {
                    if (row_id == tid) { match = true; break; }
                }
                if (!match) continue;
            } else {
                bool match = false;
                for (const auto& t : towns) {
                    if (db.col_town[i] == t) { match = true; break; }
                }
                if (!match) continue;
            }

            // year match
            if (db.col_month_year[i] != target_year) continue;

            // month in range
            const uint8_t m = db.col_month_month[i];
            if (m < start_month || m > end_month) continue;

        } else {
            // --- C4 OFF: Baseline order — Year, Month, Town ---

            // year match
            if (db.col_month_year[i] != target_year) continue;

            // month in range
            const uint8_t m = db.col_month_month[i];
            if (m < start_month || m > end_month) continue;

            // town match (A1 controls int vs string comparison)
            if (db.use_dict_encoding) {
                bool match = false;
                const uint16_t row_id = db.col_town_encoded[i];
                for (const auto& tid : town_ids) {
                    if (row_id == tid) { match = true; break; }
                }
                if (!match) continue;
            } else {
                bool match = false;
                for (const auto& t : towns) {
                    if (db.col_town[i] == t) { match = true; break; }
                }
                if (!match) continue;
            }
        }

        // floor area threshold (always last predicate, same position)
        if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;

        // ==============================================================
        // C6: Integer multiplication early-exit gate
        // Skip records whose price/area would exceed 4725 without
        // doing floating-point division. This is a cheap integer check
        // that avoids the more expensive double division below.
        // price <= 4725 * area  ⟺  price/area <= 4725
        // uint32 * uint16 fits in uint64 safely.
        // ==============================================================
        if (db.use_int_multiply) {
            if (static_cast<uint64_t>(db.col_resale_price[i]) >
                4725ULL * static_cast<uint64_t>(db.col_floor_area[i]))
                continue;
        }

        // ==============================================================
        // A4: Read pre-computed PPSM or compute on the fly
        // When enabled, the PPSM was already calculated during CSV
        // ingestion and stored in col_price_per_sqm. Otherwise we
        // divide here as in the baseline.
        // ==============================================================
        const double ppsm = db.use_precomputed_ppsm
            ? db.col_price_per_sqm[i]
            : static_cast<double>(db.col_resale_price[i]) /
              static_cast<double>(db.col_floor_area[i]);

        if (ppsm < min_ppsm) {
            min_ppsm = ppsm;
            best_i   = i;
            result.no_result = false;
        }
    }

    // === Post-scan validation (shared by all scan configs) ===

    if (result.no_result) return;

    if (min_ppsm > 4725.0) {
        result.no_result = true;
        return;
    }

    // Populate the final result from the column vectors.
    // Always use the original string columns for output (never decode).
    result.year                = db.col_month_year[best_i];
    result.month               = db.col_month_month[best_i];
    result.town                = db.col_town[best_i];
    result.block               = db.col_block[best_i];
    result.floor_area          = db.col_floor_area[best_i];
    result.flat_model          = db.col_flat_model[best_i];
    result.lease_commence_date = db.col_lease_commence_date[best_i];
    result.price_per_sqm       = min_ppsm;
}

// ============================================================================
// Preprocessing step for intermediate result reuse (C1 + C2)
// ============================================================================
// This function also benefits from A1 (dict encoding) for the town filter.
// A4 and C6 are not applied here because the cumulative table build needs
// exact PPSM values for every qualifying record, and the C6 gate would
// incorrectly discard records whose PPSM exceeds 4725 individually but
// contribute to a valid cumulative minimum at a different (x,y).
// Actually — records above 4725 can never produce a valid result, so C6
// could be used. But the table build is O(N) once, so the gain is negligible.

std::vector<std::vector<MinEntry>> buildCumulativeTable(
                const ColumnStore&              db,
                uint16_t                        target_year,
                uint8_t                         start_month,
                const std::vector<std::string>& towns
) {
    const std::size_t N = db.size();

    // Initialize a 2D table: per-offset (1..8) by area bucket (0..150)
    std::vector<std::vector<MinEntry>> per_x(9, std::vector<MinEntry>(151));

    // Pre-resolve town filter (supports both dict-encoded and string paths)
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

    // Single scan to populate per_x table
    for (std::size_t i = 0; i < N; ++i) {
        // filter 1: year match
        if (db.col_month_year[i] != target_year) continue;

        // filter 2: month in range — compute offset relative to start_month
        const uint8_t month = db.col_month_month[i];
        if (month < start_month) continue;
        int offset = static_cast<int>(month) - static_cast<int>(start_month) + 1;
        if (offset < 1 || offset > 8) continue;

        // filter 3: town match (A1 controls int vs string)
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

        // filter 4: floor area — clamp to bucket range
        const unsigned area = db.col_floor_area[i];
        if (area < 80) continue;
        const unsigned bucket = (area > 150) ? 150u : area;

        // compute ppsm (A4: use pre-computed if available)
        const double ppsm = db.use_precomputed_ppsm
            ? db.col_price_per_sqm[i]
            : static_cast<double>(db.col_resale_price[i]) /
              static_cast<double>(db.col_floor_area[i]);

        // update the per-offset, per-area entry with min
        MinEntry &entry = per_x[offset][bucket];
        if (!entry.has || ppsm < entry.ppsm) {
            entry.has  = true;
            entry.ppsm = ppsm;
            entry.idx  = i;
        }
    }

    // C1 sweep: cum_x[x][area] = min PPSM over offsets 1..x for exact area bucket
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

    // C2 sweep: propagate min from high area down so cum_x[x][y] covers >= y
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