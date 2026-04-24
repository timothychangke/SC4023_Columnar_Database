

#include <chrono>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <string>
#include <vector>

#include "../include/column_store.h"
#include "../include/csv_parser.h"
#include "../include/output_writer.h"
#include "../include/query_engine.h"
#include "../include/column_file_io.h"

class TeeBuf : public std::streambuf {
public:
    TeeBuf(std::streambuf* first, std::streambuf* second)
        : first_(first), second_(second) {}

protected:
    int overflow(int ch) override {
        if (ch == traits_type::eof()) {
            return sync() == 0 ? traits_type::not_eof(ch) : traits_type::eof();
        }

        const int r1 = first_ ? first_->sputc(static_cast<char>(ch)) : traits_type::not_eof(ch);
        const int r2 = second_ ? second_->sputc(static_cast<char>(ch)) : traits_type::not_eof(ch);
        if (traits_type::eq_int_type(r1, traits_type::eof()) ||
            traits_type::eq_int_type(r2, traits_type::eof())) {
            return traits_type::eof();
        }
        return ch;
    }

    int sync() override {
        const int s1 = first_ ? first_->pubsync() : 0;
        const int s2 = second_ ? second_->pubsync() : 0;
        return (s1 == 0 && s2 == 0) ? 0 : -1;
    }

private:
    std::streambuf* first_;
    std::streambuf* second_;
};

namespace perf {
    uint64_t rows_scanned     = 0;  // total row iterations across all queries
    uint64_t town_comparisons = 0;  // town predicate comparisons (string or int)
    uint64_t town_bitmap_lookups = 0;       // number of town bitmaps OR-ed
    uint64_t rows_eliminated_by_bitmap = 0; // rows pruned by bitmap mask
    uint64_t rows_passed      = 0;  // rows that passed all filters
    uint64_t queries_valid    = 0;  // (x,y) pairs with a valid result
    uint64_t chunks_total     = 0;  // total chunks evaluated 
    uint64_t chunks_skipped   = 0;  // chunks skipped by zone map pruning 
    uint64_t runs_scanned     = 0;  // total town runs traversed
    uint64_t rows_skipped_by_rle = 0;      // rows avoided by run pruning
    uint64_t rows_scanned_after_rle = 0;   // rows actually scanned after pruning

    void reset() {
        rows_scanned = 0;
        town_comparisons = 0;
        town_bitmap_lookups = 0;
        rows_eliminated_by_bitmap = 0;
        rows_passed = 0;
        queries_valid = 0;
        chunks_total = 0;
        chunks_skipped = 0;
        runs_scanned = 0;
        rows_skipped_by_rle = 0;
        rows_scanned_after_rle = 0;
    }
}
struct OptConfig {
    std::string name;           // display name for the table
    bool dict_encoding;         
    bool reuse;                 
    bool precompute_ppsm;       
    bool int_multiply;          
    bool predicate_reorder;     
    bool zone_maps;             
    bool presorted_storage;     
    bool rle_town;              
    bool month_binary_search;   
    bool late_materialise;      
    bool columnar_files;        
    bool        chunked_io      = false;  
    std::size_t memory_budget_mb = 50;    
    bool bitmap_index_town = false; 
    bool        mmap_io         = false;  
    bool        town_partitioning = false; 

    // apply this config to a ColumnStore before loading
    void apply(ColumnStore& db) const {
        db.use_dict_encoding     = dict_encoding;
        db.use_reuse             = reuse;
        db.use_precomputed_ppsm  = precompute_ppsm;
        db.use_int_multiply      = int_multiply;
        db.use_predicate_reorder = predicate_reorder;
        db.use_zone_maps         = zone_maps;
        db.use_presorted_storage = presorted_storage;
        db.use_rle_town          = rle_town;
        db.use_month_binary_search = month_binary_search;
        db.use_late_materialise = late_materialise;
        db.use_columnar_files = columnar_files;
        db.use_bitmap_index_town = bitmap_index_town;
        db.use_chunked_io      = chunked_io;
        db.memory_budget_bytes = memory_budget_mb * 1024 * 1024;
        db.use_mmap_io         = mmap_io;
        db.use_town_partitioning = town_partitioning;
    }
};

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

static std::size_t estimateMemoryBytes(const ColumnStore& db) {
    std::size_t bytes = 0;
    const std::size_t N = db.size();

    // numeric columns
    bytes += N * sizeof(uint16_t);  // col_month_year
    bytes += N * sizeof(uint8_t);   // col_month_month
    bytes += N * sizeof(uint16_t);  // col_floor_area
    bytes += N * sizeof(uint16_t);  // col_lease_commence_date
    bytes += N * sizeof(uint32_t);  // col_resale_price

    // string columns: sizeof(std::string) overhead + actual char data
    auto stringColBytes = [&](const std::vector<std::string>& col) -> std::size_t {
        std::size_t s = col.capacity() * sizeof(std::string);
        for (const auto& str : col) {
            s += str.capacity();  // heap-allocated chars
        }
        return s;
    };

    if (!db.use_columnar_files) {
        bytes += stringColBytes(db.col_town);
        bytes += stringColBytes(db.col_block);
        bytes += stringColBytes(db.col_street_name);
        bytes += stringColBytes(db.col_flat_type);
        bytes += stringColBytes(db.col_flat_model);
        bytes += stringColBytes(db.col_storey_range);
    }

    // encoded columns (if populated)
    bytes += db.col_town_encoded.capacity() * sizeof(uint16_t);
    bytes += db.col_flat_type_encoded.capacity() * sizeof(uint16_t);
    bytes += db.col_flat_model_encoded.capacity() * sizeof(uint16_t);
    bytes += db.col_street_name_encoded.capacity() * sizeof(uint16_t);
    // pre-computed PPSM column
    bytes += db.col_price_per_sqm.capacity() * sizeof(double);

    // dictionary overhead
    auto dictBytes = [](const DictionaryEncoder& d) -> std::size_t {
        std::size_t s = 0;
        for (const auto& str : d.id_to_str) {
            s += sizeof(std::string) + str.capacity();
        }
        // rough estimate for unordered_map overhead
        s += d.str_to_id.bucket_count() * (sizeof(void*) + sizeof(std::pair<std::string, uint16_t>));
        return s;
    };

    bytes += dictBytes(db.dict_town);
    bytes += dictBytes(db.dict_flat_type);
    bytes += dictBytes(db.dict_flat_model);
    bytes += dictBytes(db.dict_street_name);

    // zone map overhead 
    auto zmBytes = [](const ZoneMap& zm) -> std::size_t {
        return zm.chunks.capacity() * sizeof(ZoneMapEntry);
    };
    bytes += zmBytes(db.zm_floor_area);
    bytes += zmBytes(db.zm_resale_price);
    bytes += zmBytes(db.zm_month_year);
    bytes += zmBytes(db.zm_month_month);

    // bitmap index overhead 
    // vector<bool> stores bits packed; include per-town vector overhead too.
    for (const auto& bm : db.town_bitmaps) {
        bytes += sizeof(std::vector<bool>);
        bytes += (bm.capacity() + 7) / 8;
    }
    bytes += db.town_bitmap_lookup.bucket_count() *
             (sizeof(void*) + sizeof(std::pair<std::string, uint16_t>));
    // RLE overhead
    bytes += db.town_run_value_encoded.capacity() * sizeof(uint16_t);
    bytes += db.town_run_start.capacity() * sizeof(uint32_t);
    bytes += db.town_run_length.capacity() * sizeof(uint32_t);
    for (const auto& s : db.town_run_value) {
        bytes += sizeof(std::string) + s.capacity();
    }
    for (const auto& kv : db.town_to_runs_encoded) {
        bytes += sizeof(uint16_t) + kv.second.capacity() * sizeof(uint32_t);
    }
    for (const auto& kv : db.town_to_runs) {
        bytes += sizeof(std::string) + kv.first.capacity() + kv.second.capacity() * sizeof(uint32_t);
    }

    return bytes;
}

static void runQueryInstrumented(
    const ColumnStore&              db,
    int                             x,
    int                             y,
    uint16_t                        target_year,
    uint8_t                         start_month,
    const std::vector<std::string>& towns,
    const std::vector<uint8_t>*     precomputed_town_mask,
    QueryResult&                    result)
{
    const uint64_t prev_bitmap_lookups = db.town_bitmap_lookups;
    const uint64_t prev_bitmap_eliminated = db.rows_eliminated_by_bitmap;

    runQuery(db, x, y, target_year, start_month, towns, result, precomputed_town_mask);

    perf::town_bitmap_lookups += (db.town_bitmap_lookups - prev_bitmap_lookups);
    perf::rows_eliminated_by_bitmap +=
        (db.rows_eliminated_by_bitmap - prev_bitmap_eliminated);

    if (db.use_reuse) {
        if (!result.no_result) ++perf::queries_valid;
        return;
    }

    // count zone map chunk stats 
    if (db.use_zone_maps) {
        const uint8_t end_month_zm = static_cast<uint8_t>(
            std::min(static_cast<int>(start_month) + x - 1, 12));
        const std::size_t num_chunks = db.zm_floor_area.numChunks();
        const uint32_t ty32 = static_cast<uint32_t>(target_year);
        const uint32_t sm32 = static_cast<uint32_t>(start_month);
        const uint32_t em32 = static_cast<uint32_t>(end_month_zm);
        const uint32_t yt32 = static_cast<uint32_t>(y);

        for (std::size_t c = 0; c < num_chunks; ++c) {
            ++perf::chunks_total;
            if (!db.zm_month_year.chunkMayContainEQ(c, ty32) ||
                db.zm_month_month.chunks[c].max_val < sm32 ||
                db.zm_month_month.chunks[c].min_val > em32 ||
                !db.zm_floor_area.chunkMayContainGEQ(c, yt32)) {
                ++perf::chunks_skipped;
            }
        }
    }

    // count rows scanned and town comparisons for this query
    const std::size_t N = db.size();
    const uint8_t end_month = static_cast<uint8_t>(
        std::min(static_cast<int>(start_month) + x - 1, 12));

    // simulate the scan to count predicates
    // (this is a counting pass only: the real work was already done above)
    uint64_t local_rows = 0;
    uint64_t local_town_cmp = 0;
    uint64_t local_passed = 0;

    // when town_partitioning is on, the loaded data is already
    // pre-filtered: no town comparisons happen in the scan path.
    const bool e1_skip_town = db.use_town_partitioning;

    // pre-resolve town IDs if dict encoding is on
    std::vector<uint16_t> town_ids;
    std::vector<bool> town_mask;
    const bool use_bitmap_count = db.use_bitmap_index_town &&
                                  !db.town_bitmaps.empty() &&
                                  db.town_bitmaps.front().size() == N;

    if (use_bitmap_count) {
        town_mask.assign(N, false);
        if (db.use_dict_encoding) {
            for (const auto& t : towns) {
                uint16_t id = 0;
                if (!db.dict_town.lookup(t, id) || id >= db.town_bitmaps.size()) continue;
                const auto& bm = db.town_bitmaps[id];
                for (std::size_t i = 0; i < N; ++i) {
                    town_mask[i] = town_mask[i] || bm[i];
                }
            }
        } else {
            for (const auto& t : towns) {
                auto it = db.town_bitmap_lookup.find(t);
                if (it == db.town_bitmap_lookup.end() || it->second >= db.town_bitmaps.size()) continue;
                const auto& bm = db.town_bitmaps[it->second];
                for (std::size_t i = 0; i < N; ++i) {
                    town_mask[i] = town_mask[i] || bm[i];
                }
            }
        }
    }

    if (db.use_dict_encoding) {
        for (const auto& t : towns) {
            uint16_t id;
            if (db.dict_town.lookup(t, id)) town_ids.push_back(id);
        }
    }

    // fast path counting model: range scans inside town partitions.
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
                ++local_rows;
                if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;
                ++local_passed;
            }
        }

        perf::rows_scanned += local_rows;
        perf::town_comparisons += local_town_cmp;
        perf::rows_passed += local_passed;
        if (!result.no_result) ++perf::queries_valid;
        return;
    }

    // counting model: only rows inside selected town runs are scanned.
    if (db.use_rle_town && !db.town_run_start.empty()) {
        std::vector<uint32_t> selected_runs;
        if (db.use_dict_encoding) {
            for (const auto& t : towns) {
                uint16_t id = 0;
                if (!db.dict_town.lookup(t, id)) continue;
                auto it = db.town_to_runs_encoded.find(id);
                if (it == db.town_to_runs_encoded.end()) continue;
                selected_runs.insert(selected_runs.end(), it->second.begin(), it->second.end());
            }
        } else {
            for (const auto& t : towns) {
                auto it = db.town_to_runs.find(t);
                if (it == db.town_to_runs.end()) continue;
                selected_runs.insert(selected_runs.end(), it->second.begin(), it->second.end());
            }
        }

        if (!selected_runs.empty()) {
            std::sort(selected_runs.begin(), selected_runs.end());
            selected_runs.erase(std::unique(selected_runs.begin(), selected_runs.end()), selected_runs.end());

            uint64_t rows_in_runs = 0;
            const uint32_t start_key = monthKey(target_year, start_month);
            const uint32_t end_key   = monthKey(target_year, end_month);

            for (const uint32_t run_id : selected_runs) {
                ++perf::runs_scanned;
                const std::size_t run_start = db.town_run_start[run_id];
                const std::size_t run_end = run_start + db.town_run_length[run_id];
                rows_in_runs += db.town_run_length[run_id];

                std::size_t scan_l = run_start;
                std::size_t scan_r = run_end;
                if (db.use_presorted_storage && db.use_month_binary_search) {
                    scan_l = lowerBoundMonthKey(db, run_start, run_end, start_key);
                    scan_r = upperBoundMonthKey(db, scan_l, run_end, end_key);
                }

                for (std::size_t i = scan_l; i < scan_r; ++i) {
                    ++local_rows;
                    ++perf::rows_scanned_after_rle;

                    if (!(db.use_presorted_storage && db.use_month_binary_search)) {
                        if (db.col_month_year[i] != target_year) continue;
                        if (db.col_month_month[i] < start_month || db.col_month_month[i] > end_month) continue;
                    }

                    if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;
                    ++local_passed;
                }
            }

            if (rows_in_runs < N) {
                perf::rows_skipped_by_rle += static_cast<uint64_t>(N) - rows_in_runs;
            }

            perf::rows_scanned += local_rows;
            perf::town_comparisons += 0;
            perf::rows_passed += local_passed;
            if (!result.no_result) ++perf::queries_valid;
            return;
        }
    }

    for (std::size_t i = 0; i < N; ++i) {
        ++local_rows;

        if (db.use_predicate_reorder) {
            // Town → Year → Month
            if (use_bitmap_count) {
                if (!town_mask[i]) continue;
            } else if (!e1_skip_town) {
                if (db.use_dict_encoding) {
                    bool match = false;
                    for (const auto& tid : town_ids) {
                        ++local_town_cmp;
                        if (db.col_town_encoded[i] == tid) { match = true; break; }
                    }
                    if (!match) continue;
                } else {
                    bool match = false;
                    for (const auto& t : towns) {
                        ++local_town_cmp;
                        if (db.col_town[i] == t) { match = true; break; }
                    }
                    if (!match) continue;
                }
            }
            if (db.col_month_year[i] != target_year) continue;
            if (db.col_month_month[i] < start_month || db.col_month_month[i] > end_month) continue;
        } else {
            // Year → Month → Town
            if (db.col_month_year[i] != target_year) continue;
            if (db.col_month_month[i] < start_month || db.col_month_month[i] > end_month) continue;
            if (use_bitmap_count) {
                if (!town_mask[i]) continue;
            } else if (!e1_skip_town) {
                if (db.use_dict_encoding) {
                    bool match = false;
                    for (const auto& tid : town_ids) {
                        ++local_town_cmp;
                        if (db.col_town_encoded[i] == tid) { match = true; break; }
                    }
                    if (!match) continue;
                } else {
                    bool match = false;
                    for (const auto& t : towns) {
                        ++local_town_cmp;
                        if (db.col_town[i] == t) { match = true; break; }
                    }
                    if (!match) continue;
                }
            }
        }

        if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;
        ++local_passed;
    }

    perf::rows_scanned += local_rows;
    perf::town_comparisons += local_town_cmp;
    perf::rows_passed += local_passed;
    if (!result.no_result) ++perf::queries_valid;
}

struct BenchmarkResult {
    std::string config_name;
    double      load_time_ms;
    double      query_time_ms;       // average across num_runs
    double      total_time_ms;       // load + query avg
    uint64_t    rows_scanned = 0;
    uint64_t    town_comparisons = 0;
    uint64_t    town_bitmap_lookups = 0;
    uint64_t    rows_eliminated_by_bitmap = 0;
    uint64_t    rows_passed = 0;
    uint64_t    queries_valid = 0;
    uint64_t    chunks_total = 0;        // total chunks evaluated
    uint64_t    chunks_skipped = 0;      // chunks skipped by zone map pruning
    uint64_t    runs_scanned = 0;         
    uint64_t    rows_skipped_by_rle = 0;  
    uint64_t    rows_scanned_after_rle = 0; 
    std::size_t memory_bytes;
    std::size_t dict_town_size;
    std::size_t dict_flat_type_size;
    std::size_t dict_flat_model_size;
    std::size_t dict_street_size;
    std::vector<QueryResult> results; // for correctness comparison
    std::size_t io_chunks_loaded = 0;   
    std::size_t io_bytes_read    = 0;   
    std::size_t io_chunk_rows    = 0;   // peak-chunk-row count
};
static BenchmarkResult runBenchmark(
    const std::string& csv_path,
    const std::string& matric_number,
    const OptConfig&   config,
    int                num_runs)
{
    BenchmarkResult bm;
    bm.config_name = config.name;

    // derive query params (same for all configs)
    uint16_t target_year = 0;
    uint8_t  start_month = 0;
    deriveQueryParams(matric_number, target_year, start_month);
    auto towns = buildTownList(matric_number);
    std::vector<uint8_t> town_bitmap_mask;
    const std::vector<uint8_t>* town_bitmap_mask_ptr = nullptr;

    ColumnStore db;
    config.apply(db);

    // if columnar files requested, ensure they exist first (untimed)
    if (config.columnar_files) {
        // Use a config-specific directory so different flag combos don't clash
        std::string col_dir = "data/columns";
        if (config.dict_encoding)    col_dir += "_dict";
        if (config.precompute_ppsm)  col_dir += "_ppsm";
        if (config.presorted_storage) col_dir += "_a2";
        db.column_dir = col_dir;

        std::cout << "  column_dir = " << db.column_dir << "\n";
        std::cout << "  dict_encoding = " << config.dict_encoding
                  << ", precompute_ppsm = " << config.precompute_ppsm
                  << ", reuse = " << config.reuse << "\n";

        std::ifstream test_meta(db.column_dir + "/meta.col", std::ios::binary);
        if (!test_meta.is_open()) {
            std::cout << "  Column files not found, generating from CSV...\n";
            ColumnStore tmp;
            tmp.use_dict_encoding    = config.dict_encoding;
            tmp.use_precomputed_ppsm = config.precompute_ppsm;
            tmp.use_presorted_storage = config.presorted_storage;
            loadCSV(csv_path, tmp);
            writeColumnFiles(tmp, db.column_dir);
            std::cout << "  Column files written to " << db.column_dir << "\n";
        } else {
            std::cout << "  Column files found, loading...\n";
        }
        test_meta.close();
    }

    // if town partitioning requested, ensure partitioned files exist
    std::string e1_dir;
    if (config.town_partitioning) {
        e1_dir = "data/columns_e1";
        if (config.dict_encoding)     e1_dir += "_dict";
        if (config.precompute_ppsm)   e1_dir += "_ppsm";
        if (config.presorted_storage) e1_dir += "_a2";

        // Check if partition dirs exist by testing for any subdirectory
        bool e1_exists = std::filesystem::exists(e1_dir) &&
                         !std::filesystem::is_empty(e1_dir);
        if (!e1_exists) {
            std::cout << "  Partitioned files not found, generating from CSV...\n";
            ColumnStore tmp;
            tmp.use_dict_encoding    = config.dict_encoding;
            tmp.use_precomputed_ppsm = config.precompute_ppsm;
            tmp.use_presorted_storage = true; 
            loadCSV(csv_path, tmp);
            writeColumnFilesPartitioned(tmp, e1_dir);
            std::cout << "  Partitioned files written to " << e1_dir << "\n";
        } else {
            std::cout << "  Partitioned files found in " << e1_dir << "\n";
        }
    }

    auto t_load_start = std::chrono::high_resolution_clock::now();
    if (config.town_partitioning) {
        db.use_columnar_files = true; 
        loadColumnFilesPartitioned(e1_dir, towns, db);
    } else if (config.columnar_files && db.use_mmap_io) {
        loadColumnFilesMmap(db.column_dir, db);
    } else if (config.columnar_files) {
        loadColumnFiles(db.column_dir, db);
    } else {
        loadCSV(csv_path, db);
    }
    auto t_load_end = std::chrono::high_resolution_clock::now();

    // precompute chunk size once the load path knows total_rows.
    if (config.chunked_io) {
        if (!config.columnar_files || !config.dict_encoding) {
            std::cout << "  WARNING: chunked_io requires columnar_files "
                         "AND dict_encoding.\n";
            db.use_chunked_io = false;
        } else if (config.reuse) {
            std::cout << "  WARNING: chunked_io is a no-op when reuse is "
                         "enabled (reuse bypasses the scan path).\n";
            db.use_chunked_io = false;
        } else {
            db.io_chunk_rows = computeIOChunkRows(
                db.memory_budget_bytes, db.use_dict_encoding, db.use_precomputed_ppsm);
            std::cout << "  memory_budget = " << config.memory_budget_mb
                      << " MB, io_chunk_rows = " << db.io_chunk_rows
                      << ", total_rows = " << db.total_rows << "\n";
        }
    }

    if (config.mmap_io) {
        if (!config.columnar_files) {
            std::cout << "  WARNING: mmap_io requires columnar_files: disabling D2.\n";
            db.use_mmap_io = false;
        } else if (config.chunked_io) {
            std::cout << "  WARNING: mmap_io conflicts with chunked_io: disabling D2.\n";
            db.use_mmap_io = false;
        }
    }

    bm.load_time_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    bm.memory_bytes = estimateMemoryBytes(db);

    if (config.bitmap_index_town) {
        town_bitmap_mask = buildTownBitmapMask(db, towns);
        town_bitmap_mask_ptr = &town_bitmap_mask;
    }

    // build cumulative table if reuse is enabled 
    if (config.reuse) {
        db.cum_table = buildCumulativeTable(db, target_year, start_month, towns);;
    }

    // dictionary stats
    bm.dict_town_size      = db.dict_town.size();
    bm.dict_flat_type_size = db.dict_flat_type.size();
    bm.dict_flat_model_size = db.dict_flat_model.size();
    bm.dict_street_size    = db.dict_street_name.size();

    // QUERY PHASE (timed, averaged over num_runs) 
    std::vector<double> run_times;
    run_times.reserve(num_runs);

    for (int run = 0; run < num_runs; ++run) {
        perf::reset();
        db.town_bitmap_lookups = 0;
        db.town_bitmap_evaluations = 0;
        db.rows_eliminated_by_bitmap = 0;
        std::vector<QueryResult> results;
        results.reserve(8 * 71);

        auto t_q_start = std::chrono::high_resolution_clock::now();

        std::vector<std::vector<MinEntry>> run_cum_table;
        if (config.reuse) {
            db.cum_table = buildCumulativeTable(db, target_year, start_month, towns);
            perf::rows_scanned += db.size();
            for (std::size_t i = 0; i < db.size(); ++i) {
                if (db.col_month_year[i] != target_year) continue;
                int offset = static_cast<int>(db.col_month_month[i]) - static_cast<int>(start_month) + 1;
                if (offset < 1 || offset > 8) continue;
                ++perf::town_comparisons;
            }
        }

        if (db.use_chunked_io) {
            // batch runner. Pre-sizes `results` to exactly 568 entries
            // in slot order (x-1)*71 + (y-80).
            runAllQueriesChunked(db, target_year, start_month, towns, results);
            // perf counters aren't populated by the batch runner: skip them.
            for (const auto& r : results) {
                if (!r.no_result) ++perf::queries_valid;
            }
        } else {
            for (int x = 1; x <= 8; ++x) {
                for (int y = 80; y <= 150; ++y) {
                    QueryResult result;
                    runQueryInstrumented(db, x, y, target_year, start_month, towns,
                                         town_bitmap_mask_ptr, result);
                    results.push_back(result);
                }
            }
        }
        auto t_q_end = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_q_end - t_q_start).count();
        run_times.push_back(ms);

        // keep results + perf counters from last run
        if (run == num_runs - 1) {
            bm.results          = std::move(results);
            bm.rows_scanned     = perf::rows_scanned;
            bm.town_comparisons = perf::town_comparisons;
            bm.town_bitmap_lookups = perf::town_bitmap_lookups;
            bm.rows_eliminated_by_bitmap = perf::rows_eliminated_by_bitmap;
            bm.rows_passed      = perf::rows_passed;
            bm.queries_valid    = perf::queries_valid;
            bm.chunks_total     = perf::chunks_total;
            bm.chunks_skipped   = perf::chunks_skipped;
            bm.runs_scanned     = perf::runs_scanned;
            bm.rows_skipped_by_rle = perf::rows_skipped_by_rle;
            bm.rows_scanned_after_rle = perf::rows_scanned_after_rle;
        }
    }

    // compute average query time (drop best and worst if >= 5 runs)
    if (num_runs >= 5) {
        std::sort(run_times.begin(), run_times.end());
        double sum = std::accumulate(run_times.begin() + 1, run_times.end() - 1, 0.0);
        bm.query_time_ms = sum / (num_runs - 2);
    } else {
        double sum = std::accumulate(run_times.begin(), run_times.end(), 0.0);
        bm.query_time_ms = sum / num_runs;
    }

    bm.io_chunks_loaded = db.io_chunks_loaded;
    bm.io_bytes_read    = db.io_bytes_read;
    bm.io_chunk_rows    = db.io_chunk_rows;

    bm.total_time_ms = bm.load_time_ms + bm.query_time_ms;
    return bm;
}

static bool checkCorrectness(const std::vector<QueryResult>& baseline,
                             const std::vector<QueryResult>& optimised,
                             const std::string& config_name)
{
    if (baseline.size() != optimised.size()) {
        std::cerr << "[FAIL] " << config_name
                  << ": result count mismatch (" << baseline.size()
                  << " vs " << optimised.size() << ")\n";
        return false;
    }

    int mismatches = 0;
    for (std::size_t i = 0; i < baseline.size(); ++i) {
        const auto& b = baseline[i];
        const auto& o = optimised[i];

        if (b.no_result != o.no_result) {
            if (mismatches < 5) {
                std::cerr << "[FAIL] " << config_name
                          << ": no_result mismatch at (x=" << b.x << ",y=" << b.y
                          << ") baseline=" << b.no_result
                          << " opt=" << o.no_result << "\n";
            }
            ++mismatches;
            continue;
        }
        if (b.no_result) continue; // both no_result, OK

        // compare PPSM (rounded to int, like the output)
        long long b_ppsm = static_cast<long long>(b.price_per_sqm + 0.5);
        long long o_ppsm = static_cast<long long>(o.price_per_sqm + 0.5);
        if (b_ppsm != o_ppsm || b.town != o.town || b.block != o.block ||
            b.floor_area != o.floor_area || b.year != o.year || b.month != o.month) {
            if (mismatches < 5) {
                std::cerr << "[FAIL] " << config_name
                          << ": result mismatch at (x=" << b.x << ",y=" << b.y
                          << ") baseline_ppsm=" << b_ppsm
                          << " opt_ppsm=" << o_ppsm
                          << " baseline_town=" << b.town
                          << " opt_town=" << o.town << "\n";
            }
            ++mismatches;
        }
    }

    if (mismatches > 0) {
        std::cerr << "[FAIL] " << config_name << ": "
                  << mismatches << " total mismatches\n";
        return false;
    }
    return true;
}

static std::string formatBytes(std::size_t bytes) {
    if (bytes < 1024) return std::to_string(bytes) + " B";
    if (bytes < 1024 * 1024)
        return std::to_string(bytes / 1024) + " KB";
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << (bytes / (1024.0 * 1024.0)) << " MB";
    return oss.str();
}

static std::string formatCount(uint64_t n) {
    if (n < 1000) return std::to_string(n);
    if (n < 1000000) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << (n / 1000.0) << "K";
        return oss.str();
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << (n / 1000000.0) << "M";
    return oss.str();
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <path_to_csv> <MatriculationNumber> [num_runs=5] [output_file]\n";
        return 1;
    }

    const std::string csv_path = argv[1];
    const std::string matric   = argv[2];
    const int num_runs = (argc >= 4) ? std::atoi(argv[3]) : 5;
    const std::string output_file =
        (argc >= 5) ? ("results/" + std::string(argv[4])) : ("results/EvalResult_" + matric + ".txt");

    {
        std::filesystem::path out_path(output_file);
        if (out_path.has_parent_path()) {
            std::error_code ec;
            std::filesystem::create_directories(out_path.parent_path(), ec);
            if (ec) {
                std::cerr << "Failed to create output directory: "
                          << out_path.parent_path().string() << " (" << ec.message() << ")\n";
                return 1;
            }
        }
    }

    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open eval output file: " << output_file << "\n";
        return 1;
    }
    TeeBuf tee_buf(std::cout.rdbuf(), out.rdbuf());
    std::streambuf* original_cout = std::cout.rdbuf(&tee_buf);
    std::cerr << "Eval output will be written to: " << output_file << "\n";

    std::cout << "========================================\n";
    std::cout << "  HDB Column Store – Evaluation Suite\n";
    std::cout << "========================================\n";
    std::cout << "CSV file     : " << csv_path << "\n";
    std::cout << "Matric       : " << matric << "\n";
    std::cout << "Runs per cfg : " << num_runs << "\n\n";

 std::vector<OptConfig> configs = {
        { "Baseline",                                                                            false, false, false, false, false, false, false, false, false, false, false },
        { "Dict Encoding",                                                                   true,  false, false, false, false, false, false, false, false, false, false },
        { "Result Reuse",                                                                 false, true,  false, false, false, false, false, false, false, false, false },
        { "Dict+Reuse",                                                                true,  true,  false, false, false, false, false, false, false, false, false },
        { "Precompute PPSM",                                                                 false, false, true,  false, false, false, false, false, false, false, false },
        { "Int Multiply",                                                                    false, false, false, true,  false, false, false, false, false, false, false },
        { "Predicate Reorder",                                                               false, false, false, false, true,  false, false, false, false, false, false },
        { "Precompute PPSM + Int Multiply + Predicate Reorder",                        false, false, true,  true,  true,  false, false, false, false, false, false },
        { "Dict + Precompute PPSM + Int Multiply + Predicate Reorder",              true,  false, true,  true,  true,  false, false, false, false, false, false },
        { "Dict + Precompute PPSM + Int Multiply + Predicate Reorder + Reuse",                                                               true,  true,  true,  true,  true,  false, false, false, false, false, false },
        { "Zone Maps",                                                                       false, false, false, false, false, true,  false, false, false, false, false },
        { "Dict + ZoneMaps",                                                              true,  false, false, false, false, true,  false, false, false, false, false },
        { "Zone Maps + Dict + Predicate Reorder + Int Multiply + Precomputed PPSM", true,  false, true,  true,  true,  true,  false, false, false, false, false },

        { "Bitmap Town Index",                                                               false, false, false, false, false, false, false, false, false, false, false, false, 50, true },
        { "Dict + Bitmap Town",                                                           true,  false, false, false, false, false, false, false, false, false, false, false, 50, true },
        { "Presort + Bitmap Town",                                                        false, false, false, false, false, false, true,  false, false, false, false, false, 50, true },
        { "Zone Maps + Bitmap Town",                                                      false, false, false, false, false, true,  false, false, false, false, false, false, 50, true },
        { "LateMat + Bitmap Town",                                                        false, false, false, false, false, false, false, false, false, true,  false, false, 50, true },
        { "Full Scan Stack + Bitmap",                                            true,  false, false, false, true,  true,  true,  false, false, true,  false, false, 50, true },

        { "Reuse + ZoneMaps",                                                          false, true,  false, false, false, true,  false, false, false, false, false },
        { "Late Materialise",                                                                false, false, false, false, false, false, false, false, false, true,  false },
        { "Dict + LateMat",                                                                 true,  false, false, false, false, false, false, false, false, true,  false },
        { "Dict + LateMat+PredReorder",                                                  true,  false, false, false, true,  false, false, false, false, true,  false },
        { "All Scan Opts",                                                       true,  false, true,  true,  true,  false, false, false, false, true,  false },
        { "All Scan + ZoneMaps",                                                true,  false, true,  true,  true,  true,  false, false, false, true,  false },

        { "Pre-sorted Storage",                                                              false, false, false, false, false, false, true,  false, false, false, false },
        { "Month Binary Search (fallback without A2)",                                  false, false, false, false, false, false, false, false, true,  false, false },
        { "Pre-sorted + Month Binary Search",                                             false, false, false, false, false, false, true,  false, true,  false, false },
        { "Dict + Pre-sorted + Month Binary Search",                                   true,  false, false, false, false, false, true,  false, true,  false, false },

        { "Chunked I/O 50MB",                                                                true,  false, true,  true,  true,  true,  false, false, false, false, true,  true,  50 },
        { "Chunked I/O 10MB",                                                                true,  false, true,  true,  true,  true,  false, false, false, false, true,  true,  10 },
        { "Chunked I/O 5MB",                                                                 true,  false, true,  true,  true,  true,  false, false, false, false, true,  true,  5  },
        { "Chunked I/O 2MB",                                                                 true,  false, true,  true,  true,  true,  false, false, false, false, true,  true,  2  },
        { "Chunked I/O 1MB",                                                                 true,  false, true,  true,  true,  true,  false, false, false, false, true,  true,  1  },
        { "Chunked + Late Mat",                                                           true,  false, true,  true,  true,  true,  false, false, false, true,  true,  true,  50 },

        { "Mmap",                                                                  true,  false, true,  true,  true,  true,  false, false, false, false, true,  false, 50,   false, true },
        { "Mmap + everything",                                                             true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  false, 50,   false, true },
        { "Mmap + reuse",                                                                 true,  true,  true,  true,  true,  true,  false, false, false, false, true,  false, 50,   false, true },

        { "Town Partitioning",                                                               true,  false, false, false, false, false, true,  false, false, false, true,  false, 50,   false, false, true },
        { "Partitioned + Dict + Presort",                                                  true,  false, false, false, false, false, true,  false, false, false, true,  false, 50,   false, false, true },
        { "Partitioned + ZoneMaps",                                                         true,  false, true,  true,  true,  true,  true,  false, false, false, true,  false, 50,   false, false, true },
        { "Partitioned + Reuse",                                                          true,  true,  false, false, false, false, true,  false, false, false, true,  false, 50,   false, false, true },
        { "Partitioned + Everything",                                                      true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  false, 50,   false, false, true },

        { "RLE Town",                                                                        false, false, false, false, false, false, false, true,  false, false, false },
        { "Presort + RLE Town",                                                           false, false, false, false, false, false, true,  true,  false, false, false },
        { "Dict + Presort + RLE Town",                                                 true,  false, false, false, false, false, true,  true,  false, false, false },
        { "MonthBSearch + Presort + RLE",                                              false, false, false, false, false, false, true,  true,  true,  false, false },
        { "LateMat + Presort + RLE",                                                   false, false, false, false, false, false, true,  true,  false, true,  false },
        { "Full Stack",                                        true,  false, true,  true,  true,  true,  true,  true,  true,  true,  false },

        { "Columnar Files",                                                                  false, false, false, false, false, false, false, false, false, false, true  },
        { "Columnar+Dict",                                                                true,  false, false, false, false, false, false, false, false, false, true  },
        { "ColFile+Presort+MonthBSearch",                                              false, false, false, false, false, false, true,  false, true,  false, true  },
        { "ColFile+Presort+RLE+MonthBSearch",                                       false, false, false, false, false, false, true,  true,  true,  false, true  },

    };

    std::vector<BenchmarkResult> all_results;
    all_results.reserve(configs.size());

    for (const auto& cfg : configs) {
        std::cout << "--- Running: " << cfg.name << " ---\n";
        auto bm = runBenchmark(csv_path, matric, cfg, num_runs);
        all_results.push_back(std::move(bm));
        std::cout << "\n";
    }

    std::cout << "========================================\n";
    std::cout << "  CORRECTNESS CHECK\n";
    std::cout << "========================================\n";

    const auto& baseline = all_results[0];
    bool all_correct = true;
    for (std::size_t i = 1; i < all_results.size(); ++i) {
        bool ok = checkCorrectness(baseline.results, all_results[i].results,
                                   all_results[i].config_name);
        std::cout << "  " << all_results[i].config_name << ": "
                  << (ok ? "PASS (identical to baseline)" : "FAIL") << "\n";
        if (!ok) all_correct = false;
    }
    std::cout << "\n";

    std::cout << "========================================\n";
    std::cout << "  PERFORMANCE COMPARISON\n";
    std::cout << "========================================\n\n";

    // Header
    const int W_NAME = 30, W_COL = 14;
    std::cout << std::left << std::setw(W_NAME) << "Configuration"
              << std::right
              << std::setw(W_COL) << "Load (ms)"
              << std::setw(W_COL) << "Query (ms)"
              << std::setw(W_COL) << "Total (ms)"
              << std::setw(W_COL) << "Speedup"
              << std::setw(W_COL) << "Rows Scanned"
              << std::setw(W_COL) << "Town Cmps"
              << std::setw(W_COL) << "Rows Pass"
              << std::setw(W_COL) << "Valid (x,y)"
              << std::setw(W_COL) << "Memory"
              << "\n";

    std::cout << std::string(W_NAME + W_COL * 9, '-') << "\n";

    for (std::size_t i = 0; i < all_results.size(); ++i) {
        const auto& bm = all_results[i];
        double speedup = (i == 0) ? 1.0 : baseline.query_time_ms / bm.query_time_ms;

        std::ostringstream speedup_str;
        speedup_str << std::fixed << std::setprecision(2) << speedup << "x";

        std::ostringstream load_str, query_str, total_str;
        load_str  << std::fixed << std::setprecision(1) << bm.load_time_ms;
        query_str << std::fixed << std::setprecision(3) << bm.query_time_ms;
        total_str << std::fixed << std::setprecision(1) << bm.total_time_ms;

        std::cout << std::left << std::setw(W_NAME) << bm.config_name
                  << std::right
                  << std::setw(W_COL) << load_str.str()
                  << std::setw(W_COL) << query_str.str()
                  << std::setw(W_COL) << total_str.str()
                  << std::setw(W_COL) << speedup_str.str()
                  << std::setw(W_COL) << formatCount(bm.rows_scanned)
                  << std::setw(W_COL) << formatCount(bm.town_comparisons)
                  << std::setw(W_COL) << formatCount(bm.rows_passed)
                  << std::setw(W_COL) << bm.queries_valid
                  << std::setw(W_COL) << formatBytes(bm.memory_bytes)
                  << "\n";
    }

    bool any_dict = false;
    for (const auto& bm : all_results) {
        if (bm.dict_town_size > 0) { any_dict = true; break; }
    }

    if (any_dict) {
        std::cout << "\n========================================\n";
        std::cout << "  DICTIONARY ENCODING STATS\n";
        std::cout << "========================================\n";
        for (const auto& bm : all_results) {
            if (bm.dict_town_size == 0) continue;
            std::cout << "  " << bm.config_name << ":\n";
            std::cout << "    Town:        " << bm.dict_town_size << " unique values\n";
            std::cout << "    Flat_Type:   " << bm.dict_flat_type_size << " unique values\n";
            std::cout << "    Flat_Model:  " << bm.dict_flat_model_size << " unique values\n";
            std::cout << "    Street_Name: " << bm.dict_street_size << " unique values\n";
        }
    }
    bool any_zm = false;
    for (const auto& bm : all_results) {
        if (bm.chunks_total > 0) { any_zm = true; break; }
    }

    if (any_zm) {
        std::cout << "\n========================================\n";
        std::cout << "  ZONE MAP STATS\n";
        std::cout << "========================================\n";
        for (const auto& bm : all_results) {
            if (bm.chunks_total == 0) continue;
            double skip_rate = 100.0 * bm.chunks_skipped / bm.chunks_total;
            std::cout << "  " << bm.config_name << ":\n";
            std::cout << "    Chunks total:   " << formatCount(bm.chunks_total) << "\n";
            std::cout << "    Chunks skipped: " << formatCount(bm.chunks_skipped) << "\n";
            std::cout << std::fixed << std::setprecision(1);
            std::cout << "    Skip rate:      " << skip_rate << "%\n";
        }
    }

    bool any_b2 = false;
    for (const auto& bm : all_results) {
        if (bm.town_bitmap_lookups > 0 || bm.rows_eliminated_by_bitmap > 0) {
            any_b2 = true;
            break;
        }
    }

    if (any_b2) {
        std::cout << "\n========================================\n";
        std::cout << "  BITMAP INDEX STATS\n";
        std::cout << "========================================\n";
        for (const auto& bm : all_results) {
            if (bm.town_bitmap_lookups == 0 && bm.rows_eliminated_by_bitmap == 0) continue;
            std::cout << "  " << bm.config_name << ":\n";
            std::cout << "    Bitmap lookups:      " << formatCount(bm.town_bitmap_lookups) << "\n";
            std::cout << "    Rows eliminated:     " << formatCount(bm.rows_eliminated_by_bitmap) << "\n";
        }
    }

    bool any_d1 = false;
    for (const auto& bm : all_results) {
        if (bm.io_chunks_loaded > 0) { any_d1 = true; break; }
    }
    if (any_d1) {
        std::cout << "\n========================================\n";
        std::cout << "  CHUNKED I/O STATS\n";
        std::cout << "========================================\n";
        for (const auto& bm : all_results) {
            if (bm.io_chunks_loaded == 0) continue;
            std::cout << "  " << bm.config_name << ":\n";
            std::cout << "    I/O chunks loaded: " << bm.io_chunks_loaded << "\n";
            std::cout << "    I/O bytes read:    " << formatBytes(bm.io_bytes_read) << "\n";
            std::cout << "    Peak chunk rows:   " << bm.io_chunk_rows << "\n";
        }
    }

    bool any_a5 = false;
    for (const auto& bm : all_results) {
        if (bm.runs_scanned > 0 || bm.rows_skipped_by_rle > 0 || bm.rows_scanned_after_rle > 0) {
            any_a5 = true;
            break;
        }
    }
    if (any_a5) {
        std::cout << "\n========================================\n";
        std::cout << "  RLE TOWN STATS\n";
        std::cout << "========================================\n";
        for (const auto& bm : all_results) {
            if (bm.runs_scanned == 0 && bm.rows_skipped_by_rle == 0 && bm.rows_scanned_after_rle == 0) continue;
            std::cout << "  " << bm.config_name << ":\n";
            std::cout << "    Town runs scanned:    " << formatCount(bm.runs_scanned) << "\n";
            std::cout << "    Rows skipped by RLE:  " << formatCount(bm.rows_skipped_by_rle) << "\n";
            std::cout << "    Rows scanned postRLE: " << formatCount(bm.rows_scanned_after_rle) << "\n";
        }
    }

    if (all_results.size() > 1) {
        std::cout << "\n========================================\n";
        std::cout << "  DELTA ANALYSIS (vs Baseline)\n";
        std::cout << "========================================\n";

        for (std::size_t i = 1; i < all_results.size(); ++i) {
            const auto& bm = all_results[i];
            const auto& bl = baseline;

            double query_delta_pct = ((bl.query_time_ms - bm.query_time_ms) / bl.query_time_ms) * 100.0;
            double load_delta_pct  = ((bl.load_time_ms - bm.load_time_ms) / bl.load_time_ms) * 100.0;
            double mem_delta_pct   = ((double)bl.memory_bytes - (double)bm.memory_bytes) / (double)bl.memory_bytes * 100.0;
            double cmp_delta_pct   = ((double)bl.town_comparisons - (double)bm.town_comparisons) / (double)bl.town_comparisons * 100.0;

            std::cout << "\n  " << bm.config_name << ":\n";
            std::cout << std::fixed << std::setprecision(1);
            std::cout << "    Query time:       " << (query_delta_pct >= 0 ? "+" : "")
                      << query_delta_pct << "% "
                      << (query_delta_pct > 0 ? "(faster)" : "(slower)") << "\n";
            std::cout << "    Load time:        " << (load_delta_pct >= 0 ? "+" : "")
                      << load_delta_pct << "% "
                      << (load_delta_pct > 0 ? "(faster)" : "(slower)") << "\n";
            std::cout << "    Memory:           " << (mem_delta_pct >= 0 ? "+" : "")
                      << mem_delta_pct << "% "
                      << (mem_delta_pct > 0 ? "(less)" : "(more)") << "\n";
            std::cout << "    Town comparisons: " << (cmp_delta_pct >= 0 ? "+" : "")
                      << cmp_delta_pct << "% "
                      << (cmp_delta_pct > 0 ? "(fewer)" : "(more)") << "\n";
        }
    }

    std::cout << "\n========================================\n";
    std::cout << "  OVERALL: " << (all_correct ? "ALL CONFIGS CORRECT" : "SOME CONFIGS FAILED")
              << "\n========================================\n";

    const int exit_code = all_correct ? 0 : 1;
    std::cout.flush();
    std::cout.rdbuf(original_cout);
    std::cerr << "Eval report written to: " << output_file << "\n";
    return exit_code;
}