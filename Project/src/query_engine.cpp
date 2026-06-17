

#include "query_engine.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <cstdint>
#include <string>
#include <vector>
#include <sstream>
#include <unordered_set>
#include "column_file_io.h"
#include <iostream>

namespace {
std::vector<uint8_t> buildTownBitmapMaskImpl(const ColumnStore& db,
                                             const std::vector<std::string>& towns) {
    const std::size_t N = db.size();
    std::vector<uint8_t> mask(N, 0);

    if (!db.use_bitmap_index_town || db.town_bitmaps.empty()) {
        return mask;
    }

    if (db.use_dict_encoding) {
        for (const auto& t : towns) {
            uint16_t tid = 0;
            if (!db.dict_town.lookup(t, tid)) continue;
            if (tid >= db.town_bitmaps.size()) continue;

            ++db.town_bitmap_lookups;
            const auto& bm = db.town_bitmaps[tid];
            for (std::size_t i = 0; i < N; ++i) {
                mask[i] = static_cast<uint8_t>(mask[i] | static_cast<uint8_t>(bm[i]));
            }
        }
    } else {
        for (const auto& t : towns) {
            auto it = db.town_bitmap_lookup.find(t);
            if (it == db.town_bitmap_lookup.end()) continue;
            if (it->second >= db.town_bitmaps.size()) continue;

            ++db.town_bitmap_lookups;
            const auto& bm = db.town_bitmaps[it->second];
            for (std::size_t i = 0; i < N; ++i) {
                mask[i] = static_cast<uint8_t>(mask[i] | static_cast<uint8_t>(bm[i]));
            }
        }
    }

    return mask;
}

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

// Checks if a row passes both area and price thresholds.
// Returns false if area < y_threshold or ppsm > 4725, otherwise true.
inline bool passesAreaAndPriceFilters(const ColumnStore& db,
                                      uint16_t row_floor_area,
                                      uint32_t row_resale_price,
                                      int y_threshold) {
    // Area must meet minimum threshold
    if (row_floor_area < static_cast<uint16_t>(y_threshold)) {
        return false;
    }
    
    // Price per sqm must be <= 4725 (via integer multiply optimization when enabled)
    if (db.use_int_multiply) {
        if (static_cast<uint64_t>(row_resale_price) >
            4725ULL * static_cast<uint64_t>(row_floor_area)) {
            return false;
        }
    }
    
    return true;
}
} 

// Updates best candidate tracking. Parameters are references: all modifications
// propagate back to caller. Returns true if candidate was updated.
inline bool updateBestCandidate(double ppsm,
                                std::size_t row_idx,
                                double& min_ppsm,
                                std::size_t& best_i,
                                QueryResult& result) {
    if (ppsm < min_ppsm) {
        min_ppsm         = ppsm;  // by-ref: propagates to caller
        best_i           = row_idx;  // by-ref: propagates to caller
        result.local_idx = row_idx;  // by-ref: propagates to caller
        result.no_result = false;
        return true;
    }
    return false;
}

// Materializes result display fields (town, block, flat_model, lease_commence_date).
// Result is passed by-ref, so all field assignments persist in caller's result.
inline void materializeResultDetails(const ColumnStore& db,
                                     std::size_t idx,
                                     QueryResult& result,
                                     bool allow_partitioned_columnar = false,
                                    bool condition_met) {
    if (condition_met) {
    result.town = db.use_mmap_io
        ? loadStringAtMmap(db.column_dir + "/town.col", idx)
        : loadStringAt(db.column_dir + "/town.col", idx);
    result.block = db.use_mmap_io
        ? loadStringAtMmap(db.column_dir + "/block.col", idx)
        : loadStringAt(db.column_dir + "/block.col", idx);
    result.flat_model = db.use_mmap_io
        ? loadStringAtMmap(db.column_dir + "/flat_model.col", idx)
        : loadStringAt(db.column_dir + "/flat_model.col", idx);
    result.lease_commence_date = db.use_mmap_io
        ? loadUint16AtMmap(db.column_dir + "/lease_commence_date.col", idx)
        : loadUint16At(db.column_dir + "/lease_commence_date.col", idx);
    } else {
        result.town                = db.col_town[idx];
        result.block               = db.col_block[idx];
        result.flat_model          = db.col_flat_model[idx];
        result.lease_commence_date = db.col_lease_commence_date[idx];
    }
}

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

std::vector<uint8_t> buildTownBitmapMask(const ColumnStore& db,
                                         const std::vector<std::string>& towns) {
    return buildTownBitmapMaskImpl(db, towns);
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


void runQuery(const ColumnStore&              db,
              int                             x,
              int                             y,
              uint16_t                        target_year,
              uint8_t                         start_month,
              const std::vector<std::string>& towns,
              QueryResult&                    result,
              const std::vector<uint8_t>*     precomputed_town_mask) {

    result.x         = x;
    result.y         = y;
    result.no_result = true;

    // cap end month at 12 so we dont accidentally query into the next year.
    const uint8_t end_month = static_cast<uint8_t>(
        std::min(static_cast<int>(start_month) + x - 1, 12));

    double      min_ppsm = std::numeric_limits<double>::max();
    std::size_t best_i   = 0;

    const std::size_t N = db.size();

    // Bitmap setup: reuse a precomputed town mask when available 
    std::vector<uint8_t> local_town_mask;
    const std::vector<uint8_t>* town_mask_ptr = precomputed_town_mask;
    bool use_bitmap_path = db.use_bitmap_index_town &&
                           !db.town_bitmaps.empty() &&
                           db.town_bitmaps.front().size() == N;
    if (use_bitmap_path) {
        if (town_mask_ptr == nullptr) {
            local_town_mask = buildTownBitmapMaskImpl(db, towns);
            town_mask_ptr = &local_town_mask;
        }
      }
    if (db.use_rle_town) {
        db.town_runs_scanned = 0;
        db.rows_skipped_by_rle = 0;
        db.rows_scanned_after_rle = 0;
    }

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
        bool should_materialise = db.use_columnar_files && !db.use_town_partitioning;
        materializeResultDetails(db, e.idx, result, false, should_materialise);
        return;
    }

    if (db.use_rle_town && !db.town_run_start.empty()) {
        std::vector<uint32_t> selected_runs;
        selected_runs.reserve(towns.size());

        if (db.use_dict_encoding) {
            for (const auto& town : towns) {
                uint16_t tid = 0;
                if (!db.dict_town.lookup(town, tid)) continue;
                auto it = db.town_to_runs_encoded.find(tid);
                if (it == db.town_to_runs_encoded.end()) continue;
                selected_runs.insert(selected_runs.end(), it->second.begin(), it->second.end());
            }
        } else {
            for (const auto& town : towns) {
                auto it = db.town_to_runs.find(town);
                if (it == db.town_to_runs.end()) continue;
                selected_runs.insert(selected_runs.end(), it->second.begin(), it->second.end());
            }
        }

        if (selected_runs.empty()) {
            db.rows_skipped_by_rle = static_cast<uint64_t>(N);
            return;
        }

        std::sort(selected_runs.begin(), selected_runs.end());
        selected_runs.erase(std::unique(selected_runs.begin(), selected_runs.end()), selected_runs.end());

        uint64_t rows_in_selected_runs = 0;
        for (const uint32_t run_id : selected_runs) {
            rows_in_selected_runs += static_cast<uint64_t>(db.town_run_length[run_id]);
        }
        db.rows_skipped_by_rle = (rows_in_selected_runs >= N)
            ? 0
            : static_cast<uint64_t>(N) - rows_in_selected_runs;

        const uint32_t start_key = monthKey(target_year, start_month);
        const uint32_t end_key   = monthKey(target_year, end_month);

        for (const uint32_t run_id : selected_runs) {
            ++db.town_runs_scanned;

            const std::size_t run_start = db.town_run_start[run_id];
            const std::size_t run_end   = run_start + db.town_run_length[run_id];

            std::size_t scan_l = run_start;
            std::size_t scan_r = run_end;

            if (db.use_presorted_storage && db.use_month_binary_search) {
                scan_l = lowerBoundMonthKey(db, run_start, run_end, start_key);
                scan_r = upperBoundMonthKey(db, scan_l, run_end, end_key);
            }

            for (std::size_t i = scan_l; i < scan_r; ++i) {
                ++db.rows_scanned_after_rle;

                if (!(db.use_presorted_storage && db.use_month_binary_search)) {
                    if (db.col_month_year[i] != target_year) continue;
                    const uint8_t m = db.col_month_month[i];
                    if (m < start_month || m > end_month) continue;
                }

                if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;

                if (!passesAreaAndPriceFilters(db, db.col_floor_area[i], db.col_resale_price[i], y)) {
                    continue;
                }

                const double ppsm = db.use_precomputed_ppsm
                    ? db.col_price_per_sqm[i]
                    : static_cast<double>(db.col_resale_price[i]) /
                      static_cast<double>(db.col_floor_area[i]);

                updateBestCandidate(ppsm, i, min_ppsm, best_i, result);
            }
        }

        if (result.no_result) return;

        if (min_ppsm > 4725.0) {
            result.no_result = true;
            return;
        }

        result.year          = db.col_month_year[best_i];
        result.month         = db.col_month_month[best_i];
        result.floor_area    = db.col_floor_area[best_i];
        result.price_per_sqm = min_ppsm;

        materializeResultDetails(db, best_i, result, true, db.use_columnar_files);
        return;
    }

    if (db.use_presorted_storage && db.use_month_binary_search) {
        const uint32_t start_key = monthKey(target_year, start_month);
        const uint32_t end_key   = monthKey(target_year, end_month);

        for (const auto& town : towns) {
            TownPartition part;
            bool has_part = false;

            //Dict Encoding Optimisation: if dict encoding is on, compare town IDs (int==int) instead of full strings
            if (db.use_dict_encoding) {
                uint16_t tid = 0;
                if (db.dict_town.lookup(town, tid) &&
                    tid < db.town_partitions_encoded.size() &&
                    db.town_partitions_encoded[tid].valid) {
                    part = db.town_partitions_encoded[tid];
                    has_part = true;
                }
            // Standard path
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

                // cheap integer check before division.
                // If price > 4725 * area, PPSM must be > 4725, so skip early.
                if (!passesAreaAndPriceFilters(db, db.col_floor_area[i], db.col_resale_price[i], y)) {
                    continue;
                }

                const double ppsm = db.use_precomputed_ppsm
                    ? db.col_price_per_sqm[i]
                    : static_cast<double>(db.col_resale_price[i]) /
                      static_cast<double>(db.col_floor_area[i]);

                updateBestCandidate(ppsm, i, min_ppsm, best_i, result);
            }
        }

        if (result.no_result) return;

        if (min_ppsm > 4725.0) {
            result.no_result = true;
            return;
        }

        result.year                = db.col_month_year[best_i];
        result.month               = db.col_month_month[best_i];
        result.floor_area          = db.col_floor_area[best_i];
        result.price_per_sqm       = min_ppsm;

        bool should_materialize = db.use_columnar_files && !db.use_town_partitioning;
        materializeResultDetails(db, best_i, result, false, should_materialize);
        return;
    }


    // Dict Encoding setup: pre-resolve town IDs once (outside the loop)
    std::vector<uint16_t> town_ids;
    if (db.use_dict_encoding && !use_bitmap_path && !db.use_town_partitioning) {
        town_ids.reserve(towns.size());
        for (const auto& t : towns) {
            uint16_t id;
            if (db.dict_town.lookup(t, id)) {
                town_ids.push_back(id);
            }
        }
        if (town_ids.empty()) return;
    }

    const std::size_t num_chunks = db.use_zone_maps
        ? db.zm_floor_area.numChunks()
        : 1;

    const uint32_t target_year_32 = static_cast<uint32_t>(target_year);
    const uint32_t start_month_32 = static_cast<uint32_t>(start_month);
    const uint32_t end_month_32   = static_cast<uint32_t>(end_month);
    const uint32_t y_threshold    = static_cast<uint32_t>(y);

    // Late Mat setup: late materialisation survivor list 
    std::vector<size_t> survivors;
    if (db.use_late_materialise) {
        survivors.reserve(N / 20);
    }

    for (std::size_t chunk = 0; chunk < num_chunks; ++chunk) {

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

        // row bounds for this chunk 
        const std::size_t row_start = db.use_zone_maps ? chunk * ZONE_CHUNK_SIZE : 0;
        const std::size_t row_end   = db.use_zone_maps
            ? std::min(row_start + ZONE_CHUNK_SIZE, N)
            : N;

        for (std::size_t i = row_start; i < row_end; ++i) {


            if (use_bitmap_path) {
                ++db.town_bitmap_evaluations;
                if (!(*town_mask_ptr)[i]) {
                    ++db.rows_eliminated_by_bitmap;
                    continue;
                }
            }

            if (db.use_predicate_reorder) {
                // Predicate Reordering ON: Town FIRST (eliminates ~80% of rows)
                if (!use_bitmap_path && !db.use_town_partitioning) {
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

                // then year and month
                if (db.col_month_year[i] != target_year) continue;
                const uint8_t m = db.col_month_month[i];
                if (m < start_month || m > end_month) continue;

            } else {
                // Predicate Reordering OFF: baseline order: Year → Month → Town 
                if (db.col_month_year[i] != target_year) continue;

                const uint8_t m = db.col_month_month[i];
                if (m < start_month || m > end_month) continue;

                // town match 
                // skip when town_partitioning is on: data is pre-filtered
                if (!use_bitmap_path && !db.use_town_partitioning) {
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
            }

            if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;

            // Late Materialisation: defer price access 
            if (db.use_late_materialise) {
                survivors.push_back(i);
            } else {
                // Integer Multiplication Trick 
                if (!passesAreaAndPriceFilters(db, db.col_floor_area[i], db.col_resale_price[i], y)) {
                    continue;
                }

                // Pre-computed PPSM vs. on-the-fly division 
                const double ppsm = db.use_precomputed_ppsm
                    ? db.col_price_per_sqm[i]
                    : static_cast<double>(db.col_resale_price[i]) /
                      static_cast<double>(db.col_floor_area[i]);

                updateBestCandidate(ppsm, i, min_ppsm, best_i, result);
            }
        } // end inner loop (rows)
    } // end outer loop (chunks)

    if (db.use_late_materialise) {
        for (const size_t idx : survivors) {
            // integer gate on survivors
            if (!passesAreaAndPriceFilters(db, db.col_floor_area[idx], db.col_resale_price[idx], y)) {
                continue;
            }

            // precomputed vs on-the-fly
            const double ppsm = db.use_precomputed_ppsm
                ? db.col_price_per_sqm[idx]
                : static_cast<double>(db.col_resale_price[idx]) /
                  static_cast<double>(db.col_floor_area[idx]);

            updateBestCandidate(ppsm, idx, min_ppsm, best_i, result);
        }
    }

    if (result.no_result) return;
    if (min_ppsm > 4725.0) {
        result.no_result = true;
        return;
    }

    result.year       = db.col_month_year[best_i];
    result.month      = db.col_month_month[best_i];
    result.floor_area = db.col_floor_area[best_i];
    result.price_per_sqm = min_ppsm;

    bool should_materialise = db.use_columnar_files && !db.use_town_partitioning;
    materializeResultDetails(db, best_i, result, false, should_materialise);
}


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

        // filter 2: month in range: compute offset relative to start_month
        const uint8_t month = db.col_month_month[i];
        if (month < start_month) continue;
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

        // filter 4: floor area: clamp to bucket range
        const unsigned area = db.col_floor_area[i];
        if (area < 80) continue;
        const unsigned bucket = (area > 150) ? 150u : area;

        // compute ppsm (use pre-computed if available)
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

    // Reuse sweep: cum_x[x][area] = min PPSM over offsets 1..x for exact area bucket
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

    // Reuse sweep: propagate min from high area down so cum_x[x][y] covers >= y
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
        part.use_bitmap_index_town = base_db.use_bitmap_index_town;
        part.use_rle_town          = base_db.use_rle_town;
        part.use_columnar_files    = true;
        part.column_dir            = base_db.column_dir;

        // Dictionaries must be shared so encoded town IDs resolve correctly
        // across chunks. Copy by value: cheap (~26 towns, ~20 flat models).
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

        // rebuild zone maps for this chunk (cheap: O(this_chunk_rows)).
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

                    results[slot].year         = chunk_result.year;
                    results[slot].month        = chunk_result.month;
                    results[slot].floor_area   = chunk_result.floor_area;
                    results[slot].price_per_sqm = chunk_result.price_per_sqm;
                    results[slot].no_result    = false;
                }
            }
        }
    }

    // MATERIALISATION: lazy load display columns for the global winners.
    for (std::size_t slot = 0; slot < NSLOTS; ++slot) {
        if (results[slot].no_result) continue;
        const std::size_t g = best_global_idx[slot];
        results[slot].town       = base_db.use_mmap_io
            ? loadStringAtMmap(base_db.column_dir + "/town.col", g)
            : loadStringAt(base_db.column_dir + "/town.col", g);
        results[slot].block      = base_db.use_mmap_io
            ? loadStringAtMmap(base_db.column_dir + "/block.col", g)
            : loadStringAt(base_db.column_dir + "/block.col", g);
        results[slot].flat_model = base_db.use_mmap_io
            ? loadStringAtMmap(base_db.column_dir + "/flat_model.col", g)
            : loadStringAt(base_db.column_dir + "/flat_model.col", g);
        results[slot].lease_commence_date = base_db.use_mmap_io
            ? loadUint16AtMmap(base_db.column_dir + "/lease_commence_date.col", g)
            : loadUint16At(base_db.column_dir + "/lease_commence_date.col", g);
    }
}