/*
 * implementation for the parameter extraction and columnar scan.
 *
 * === ARCHITECTURE: Composable Optimisation Flags ===
 * runQuery has two paths:
 *
 * 1. REUSE PATH (use_reuse) — O(1) cumulative table lookup, early return.
 *
 * 2. SCAN PATH — single unified loop where each flag controls one decision:
 *      use_zone_maps        (B1) — outer chunk iteration: skip entire chunks
 *      use_dict_encoding    (A1) — town comparison: int==int vs string==string
 *      use_predicate_reorder(C4) — predicate order: Town-first vs Year-first
 *      use_int_multiply     (C6) — integer early-exit gate before PPSM calc
 *      use_precomputed_ppsm (A4) — PPSM source: pre-stored vs on-the-fly
 *      use_late_materialise (C3) — defer price column to Phase 2 survivors loop
 *
 * Any combination of scan-path flags just works. The reuse path is the only
 * one that short-circuits since it bypasses the scan altogether.
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
#include "column_file_io.h"
#include <iostream>

namespace {
inline uint32_t monthKey(uint16_t year, uint8_t month) {
    return static_cast<uint32_t>(year) * 12u + static_cast<uint32_t>(month - 1u);
}

inline uint32_t monthKeyAt(const ColumnStore& db, std::size_t idx) {
    return monthKey(db.col_month_year[idx], db.col_month_month[idx]);
}

std::size_t lowerBoundMonthKey(const ColumnStore& db,
                               std::size_t l,
                               std::size_t r,
                               uint32_t key) {
    while (l < r) {
        const std::size_t mid = l + (r - l) / 2;
        if (monthKeyAt(db, mid) < key) {
            l = mid + 1;
        } else {
            r = mid;
        }
    }
    return l;
}

std::size_t upperBoundMonthKey(const ColumnStore& db,
                               std::size_t l,
                               std::size_t r,
                               uint32_t key) {
    while (l < r) {
        const std::size_t mid = l + (r - l) / 2;
        if (monthKeyAt(db, mid) <= key) {
            l = mid + 1;
        } else {
            r = mid;
        }
    }
    return l;
}
} // namespace

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

    // cap end month at 12 so we dont accidentally query into the next year.
    const uint8_t end_month = static_cast<uint8_t>(
        std::min(static_cast<int>(start_month) + x - 1, 12));

    double      min_ppsm = std::numeric_limits<double>::max();
    std::size_t best_i   = 0;

    const std::size_t N = db.size();

    // =================================================================
    // REUSE PATH — O(1) table lookup, bypasses scan entirely
    // This is the only path that short-circuits. If reuse is on and the
    // cumulative table is built, we just read the answer. All other flags
    // are irrelevant since there is no scan loop to optimise.
    // =================================================================
    if (db.use_reuse && !db.cum_table.empty()) {
        const MinEntry &e = db.cum_table[x][y];
        if (!e.has) return;

        result.no_result       = false;
        result.year            = db.col_month_year[e.idx];
        result.month           = db.col_month_month[e.idx];
        result.floor_area      = db.col_floor_area[e.idx];
        result.price_per_sqm   = e.ppsm;

        if (e.ppsm > 4725.0) {
            result.no_result = true;
            return;
        }

        if (db.use_columnar_files) {
            result.town                = loadStringAt(db.column_dir + "/town.col", e.idx);
            result.block               = loadStringAt(db.column_dir + "/block.col", e.idx);
            result.flat_model          = loadStringAt(db.column_dir + "/flat_model.col", e.idx);
            result.lease_commence_date = loadUint16At(db.column_dir + "/lease_commence_date.col", e.idx);
        } else {
            result.town                = db.col_town[e.idx];
            result.block               = db.col_block[e.idx];
            result.flat_model          = db.col_flat_model[e.idx];
            result.lease_commence_date = db.col_lease_commence_date[e.idx];
        }
        return;
    }

    // =================================================================
    // A2 + B3 FAST PATH — town partitions + month binary-search range
    // Enabled only when BOTH flags are on. This keeps flag behavior
    // extensible: either flag alone falls back to the normal scan path.
    // =================================================================
    if (db.use_presorted_storage && db.use_month_binary_search) {
        const uint32_t start_key = monthKey(target_year, start_month);
        const uint32_t end_key   = monthKey(target_year, end_month);

        for (const auto& town : towns) {
            TownPartition part;
            bool has_part = false;

            if (db.use_dict_encoding) {
                uint16_t tid = 0;
                if (db.dict_town.lookup(town, tid) &&
                    tid < db.town_partitions_encoded.size() &&
                    db.town_partitions_encoded[tid].valid) {
                    part = db.town_partitions_encoded[tid];
                    has_part = true;
                }
            } else {
                auto it = db.town_partitions.find(town);
                if (it != db.town_partitions.end() && it->second.valid) {
                    part = it->second;
                    has_part = true;
                }
            }

            if (!has_part) continue;

            const std::size_t lb = lowerBoundMonthKey(db, part.begin, part.end, start_key);
            const std::size_t ub = upperBoundMonthKey(db, lb, part.end, end_key);

            for (std::size_t i = lb; i < ub; ++i) {
                if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;

                if (db.use_int_multiply) {
                    if (static_cast<uint64_t>(db.col_resale_price[i]) >
                        4725ULL * static_cast<uint64_t>(db.col_floor_area[i])) {
                        continue;
                    }
                }

                const double ppsm = db.use_precomputed_ppsm
                    ? db.col_price_per_sqm[i]
                    : static_cast<double>(db.col_resale_price[i]) /
                      static_cast<double>(db.col_floor_area[i]);

                if (ppsm < min_ppsm) {
                    min_ppsm = ppsm;
                    best_i   = i;
                    result.local_idx = i;  // A9: needed for columnar file lazy materialisation
                    result.no_result = false;
                }
            }
        }

        if (result.no_result) return;

        if (min_ppsm > 4725.0) {
            result.no_result = true;
            return;
        }

        result.year                = db.col_month_year[best_i];
        result.month               = db.col_month_month[best_i];
        result.town                = db.col_town[best_i];
        result.block               = db.col_block[best_i];
        result.floor_area          = db.col_floor_area[best_i];
        result.flat_model          = db.col_flat_model[best_i];
        result.lease_commence_date = db.col_lease_commence_date[best_i];
        result.price_per_sqm       = min_ppsm;
        return;
    }

    // =================================================================
    // SCAN PATH — single unified loop, ALL flags compose inside
    //
    // Flag roles (each controls one isolated decision):
    //   use_zone_maps        (B1) — outer iteration: skip entire chunks
    //   use_dict_encoding    (A1) — town comparison: int==int vs string==string
    //   use_predicate_reorder(C4) — predicate order: Town-first vs Year-first
    //   use_int_multiply     (C6) — integer early-exit gate before PPSM calc
    //   use_precomputed_ppsm (A4) — PPSM source: pre-stored vs divide
    //
    // Any combination of flags just works.
    // =================================================================

    // --- A1 setup: pre-resolve town IDs once (outside the loop) ---
    std::vector<uint16_t> town_ids;
    if (db.use_dict_encoding) {
        town_ids.reserve(towns.size());
        for (const auto& t : towns) {
            uint16_t id;
            if (db.dict_town.lookup(t, id)) {
                town_ids.push_back(id);
            }
        }
        if (town_ids.empty()) return;
    }

    // --- B1 setup: chunk iteration bounds ---
    // When zone maps are OFF:  1 chunk covering [0, N) — same as a flat loop
    // When zone maps are ON:   ceil(N/CHUNK_SIZE) chunks, each pruned by min/max
    const std::size_t num_chunks = db.use_zone_maps
        ? db.zm_floor_area.numChunks()
        : 1;

    const uint32_t target_year_32 = static_cast<uint32_t>(target_year);
    const uint32_t start_month_32 = static_cast<uint32_t>(start_month);
    const uint32_t end_month_32   = static_cast<uint32_t>(end_month);
    const uint32_t y_threshold    = static_cast<uint32_t>(y);

    // --- C3 setup: late materialisation survivor list ---
    std::vector<size_t> survivors;
    if (db.use_late_materialise) {
        survivors.reserve(N / 20);
    }

    // ====================== OUTER LOOP: chunks ======================
    for (std::size_t chunk = 0; chunk < num_chunks; ++chunk) {

        // --- B1: zone map chunk pruning (skipped entirely when flag is off) ---
        if (db.use_zone_maps) {
            // year: if no row in this chunk has the target year, skip
            if (!db.zm_month_year.chunkMayContainEQ(chunk, target_year_32))
                continue;
            // month: if chunk's month range has no overlap with [start, end], skip
            if (db.zm_month_month.chunks[chunk].max_val < start_month_32 ||
                db.zm_month_month.chunks[chunk].min_val > end_month_32)
                continue;
            // area: if max area in chunk < y, no row can pass area >= y
            if (!db.zm_floor_area.chunkMayContainGEQ(chunk, y_threshold))
                continue;
        }

        // --- row bounds for this chunk ---
        const std::size_t row_start = db.use_zone_maps ? chunk * ZONE_CHUNK_SIZE : 0;
        const std::size_t row_end   = db.use_zone_maps
            ? std::min(row_start + ZONE_CHUNK_SIZE, N)
            : N;

        // =================== INNER LOOP: rows =======================
        for (std::size_t i = row_start; i < row_end; ++i) {

            // ---- PREDICATE BLOCK ----
            // C4 controls the ORDER of predicates.
            // A1 controls HOW the town predicate is evaluated.
            // These two are orthogonal — every combination works.

            if (db.use_predicate_reorder) {
                // --- C4 ON: Town FIRST (eliminates ~80% of rows) ---
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

                // then year and month
                if (db.col_month_year[i] != target_year) continue;
                const uint8_t m = db.col_month_month[i];
                if (m < start_month || m > end_month) continue;

            } else {
                // --- C4 OFF: baseline order — Year → Month → Town ---
                if (db.col_month_year[i] != target_year) continue;

                const uint8_t m = db.col_month_month[i];
                if (m < start_month || m > end_month) continue;

                // town match (A1 controls int vs string)
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

            // floor area threshold (same position regardless of C4)
            if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;

            // ---- C3: Late Materialisation — defer price access ----
            if (db.use_late_materialise) {
                survivors.push_back(i);
            } else {
                // ---- C6: Integer Multiplication Trick ----
                if (db.use_int_multiply) {
                    if (static_cast<uint64_t>(db.col_resale_price[i]) >
                        4725ULL * static_cast<uint64_t>(db.col_floor_area[i]))
                        continue;
                }

                // ---- A4: Pre-computed PPSM vs. on-the-fly division ----
                const double ppsm = db.use_precomputed_ppsm
                    ? db.col_price_per_sqm[i]
                    : static_cast<double>(db.col_resale_price[i]) /
                      static_cast<double>(db.col_floor_area[i]);

                if (ppsm < min_ppsm) {
                    min_ppsm = ppsm;
                    best_i   = i;
                    result.local_idx = i; 
                    result.no_result = false;
                }
            }
        } // end inner loop (rows)
    } // end outer loop (chunks)

    // =================================================================
    // C3 PHASE 2: fetch price column only for survivors
    // C6 and A4 compose here — they only touch col_resale_price and
    // col_floor_area, which are exactly the columns being deferred.
    // =================================================================
    if (db.use_late_materialise) {
        for (const size_t idx : survivors) {
            // C6: integer gate on survivors
            if (db.use_int_multiply) {
                if (static_cast<uint64_t>(db.col_resale_price[idx]) >
                    4725ULL * static_cast<uint64_t>(db.col_floor_area[idx]))
                    continue;
            }

            // A4: precomputed vs on-the-fly
            const double ppsm = db.use_precomputed_ppsm
                ? db.col_price_per_sqm[idx]
                : static_cast<double>(db.col_resale_price[idx]) /
                  static_cast<double>(db.col_floor_area[idx]);

            if (ppsm < min_ppsm) {
                min_ppsm = ppsm;
                best_i   = idx;
                result.local_idx = idx; 
                result.no_result = false;
            }
        }
    }

    // =================================================================
    // POST-SCAN VALIDATION (unchanged, shared by all scan configs)
    // =================================================================
    if (result.no_result) return;
    if (min_ppsm > 4725.0) {
        result.no_result = true;
        return;
    }

    result.year       = db.col_month_year[best_i];
    result.month      = db.col_month_month[best_i];
    result.floor_area = db.col_floor_area[best_i];
    result.price_per_sqm = min_ppsm;

    if (db.use_columnar_files) {
        // A9: lazy materialisation — read only the winning row from disk
        result.town                = loadStringAt(db.column_dir + "/town.col", best_i);
        result.block               = loadStringAt(db.column_dir + "/block.col", best_i);
        result.flat_model          = loadStringAt(db.column_dir + "/flat_model.col", best_i);
        result.lease_commence_date = loadUint16At(db.column_dir + "/lease_commence_date.col", best_i);
    } else {
        result.town                = db.col_town[best_i];
        result.block               = db.col_block[best_i];
        result.flat_model          = db.col_flat_model[best_i];
        result.lease_commence_date = db.col_lease_commence_date[best_i];
    }
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

// ============================================================================
// D1: Chunked I/O batch runner
// ============================================================================
//
// Reverses the normal loop nesting: instead of
//   for each (x,y): for each chunk: scan
// we do
//   for each chunk: for each (x,y): scan that chunk and fold min
//
// This ensures each chunk is read from disk exactly ONCE per benchmark run,
// not 568 times. Correctness relies on min being associative: the global
// minimum is the minimum of the per-chunk minima.
//
// Requires: use_columnar_files && use_dict_encoding.
// Incompatible with: use_reuse (caller must guard).
// ============================================================================

void runAllQueriesChunked(const ColumnStore&              base_db,
                          uint16_t                        target_year,
                          uint8_t                         start_month,
                          const std::vector<std::string>& towns,
                          std::vector<QueryResult>&       results) {

    const std::size_t N          = base_db.total_rows;
    const std::size_t chunk_rows = base_db.io_chunk_rows;

    // 568 slots: (x in [1..8]) * (y in [80..150]) = 8 * 71
    constexpr std::size_t NSLOTS = 8 * 71;
    results.assign(NSLOTS, QueryResult{});
    for (int x = 1; x <= 8; ++x) {
        for (int y = 80; y <= 150; ++y) {
            const std::size_t slot = (x - 1) * 71 + (y - 80);
            results[slot].x         = x;
            results[slot].y         = y;
            results[slot].no_result = true;
        }
    }

    std::vector<double>      min_ppsm(NSLOTS, std::numeric_limits<double>::max());
    std::vector<std::size_t> best_global_idx(NSLOTS, 0);

    // Reset observability counters
    base_db.io_chunks_loaded = 0;
    base_db.io_bytes_read    = 0;

    // ============================= OUTER LOOP: chunks =======================
    for (std::size_t chunk_start = 0; chunk_start < N; chunk_start += chunk_rows) {
        const std::size_t this_chunk_rows = std::min(chunk_rows, N - chunk_start);

        // Build a partial ColumnStore holding only this chunk's filter columns.
        ColumnStore part;
        part.use_dict_encoding     = base_db.use_dict_encoding;
        part.use_precomputed_ppsm  = base_db.use_precomputed_ppsm;
        part.use_int_multiply      = base_db.use_int_multiply;
        part.use_predicate_reorder = base_db.use_predicate_reorder;
        part.use_late_materialise  = base_db.use_late_materialise;
        part.use_zone_maps         = base_db.use_zone_maps;
        part.use_columnar_files    = true;
        part.column_dir            = base_db.column_dir;

        // Dictionaries must be shared so encoded town IDs resolve correctly
        // across chunks. Copy by value — cheap (~26 towns, ~20 flat models).
        part.dict_town        = base_db.dict_town;
        part.dict_flat_type   = base_db.dict_flat_type;
        part.dict_flat_model  = base_db.dict_flat_model;
        part.dict_street_name = base_db.dict_street_name;

        // Propagate byte counter up to base_db for reporting.
        const std::size_t before_bytes = base_db.io_bytes_read;
        loadColumnFilesChunk(base_db.column_dir, chunk_start, this_chunk_rows, part);

        // loadColumnFilesChunk writes into part.io_bytes_read; lift it up.
        base_db.io_bytes_read = before_bytes + part.io_bytes_read;
        base_db.io_chunks_loaded++;

        // B1: rebuild zone maps for this chunk (cheap: O(this_chunk_rows)).
        if (base_db.use_zone_maps) {
            const std::size_t nc =
                (this_chunk_rows + ZONE_CHUNK_SIZE - 1) / ZONE_CHUNK_SIZE;

            auto buildZM16 = [&](const std::vector<uint16_t>& col) -> ZoneMap {
                ZoneMap zm;
                zm.chunks.resize(nc);
                for (std::size_t i = 0; i < col.size(); ++i) {
                    std::size_t c = i / ZONE_CHUNK_SIZE;
                    uint32_t v = static_cast<uint32_t>(col[i]);
                    if (v < zm.chunks[c].min_val) zm.chunks[c].min_val = v;
                    if (v > zm.chunks[c].max_val) zm.chunks[c].max_val = v;
                }
                return zm;
            };
            auto buildZM8 = [&](const std::vector<uint8_t>& col) -> ZoneMap {
                ZoneMap zm;
                zm.chunks.resize(nc);
                for (std::size_t i = 0; i < col.size(); ++i) {
                    std::size_t c = i / ZONE_CHUNK_SIZE;
                    uint32_t v = static_cast<uint32_t>(col[i]);
                    if (v < zm.chunks[c].min_val) zm.chunks[c].min_val = v;
                    if (v > zm.chunks[c].max_val) zm.chunks[c].max_val = v;
                }
                return zm;
            };
            auto buildZM32 = [&](const std::vector<uint32_t>& col) -> ZoneMap {
                ZoneMap zm;
                zm.chunks.resize(nc);
                for (std::size_t i = 0; i < col.size(); ++i) {
                    std::size_t c = i / ZONE_CHUNK_SIZE;
                    uint32_t v = col[i];
                    if (v < zm.chunks[c].min_val) zm.chunks[c].min_val = v;
                    if (v > zm.chunks[c].max_val) zm.chunks[c].max_val = v;
                }
                return zm;
            };

            part.zm_floor_area   = buildZM16(part.col_floor_area);
            part.zm_resale_price = buildZM32(part.col_resale_price);
            part.zm_month_year   = buildZM16(part.col_month_year);
            part.zm_month_month  = buildZM8 (part.col_month_month);
        }


        // INNER LOOP: every (x,y) pair against this chunk.
        {
            QueryResult chunk_result;
            runQuery(part, 1, 80, target_year, start_month, towns, chunk_result);
        }
        for (int x = 1; x <= 8; ++x) {
            for (int y = 80; y <= 150; ++y) {
                const std::size_t slot = (x - 1) * 71 + (y - 80);

                QueryResult chunk_result;
                runQuery(part, x, y, target_year, start_month, towns, chunk_result);

                if (!chunk_result.no_result &&
                    chunk_result.price_per_sqm < min_ppsm[slot]) {
                    min_ppsm[slot]        = chunk_result.price_per_sqm;
                    best_global_idx[slot] = chunk_start + chunk_result.local_idx;

                    // Copy filter-side fields (year/month/floor_area/ppsm).
                    // Display fields (town/block/...) are deferred to the
                    // materialisation pass below.
                    results[slot].year         = chunk_result.year;
                    results[slot].month        = chunk_result.month;
                    results[slot].floor_area   = chunk_result.floor_area;
                    results[slot].price_per_sqm = chunk_result.price_per_sqm;
                    results[slot].no_result    = false;
                }
            }
        }

        // `part` goes out of scope here — all per-chunk memory is freed.
    }

    // MATERIALISATION: lazy load display columns for the global winners.
    for (std::size_t slot = 0; slot < NSLOTS; ++slot) {
        if (results[slot].no_result) continue;
        const std::size_t g = best_global_idx[slot];
        results[slot].town       = loadStringAt(base_db.column_dir + "/town.col", g);
        results[slot].block      = loadStringAt(base_db.column_dir + "/block.col", g);
        results[slot].flat_model = loadStringAt(base_db.column_dir + "/flat_model.col", g);
        results[slot].lease_commence_date =
            loadUint16At(base_db.column_dir + "/lease_commence_date.col", g);
    }
}