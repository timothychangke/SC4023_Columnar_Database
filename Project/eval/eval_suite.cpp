/**
 * =============================================================================
 * eval_suite.cpp
 * =============================================================================
 * Evaluation / benchmarking suite for the HDB Column Store project.
 *
 * Purpose:
 *   - Verify output correctness of each optimisation config against baseline
 *   - Measure wall-clock time (avg of multiple runs)
 *   - Measure rows scanned, comparisons made, columns accessed
 *   - Measure memory footprint of column store
 *   - Print a comparison table suitable for the project report
 *
 * Usage:
 *   ./eval_suite <path_to_csv> <MatriculationNumber> [num_runs]
 *   eg ./eval_suite ../data/ResalePricesSingapore.csv A5656567B 5
 *
 * Design:
 *   Each optimisation configuration is defined as an OptConfig struct.
 *   To add a new optimisation, just append a new entry to the CONFIGS vector.
 *   The suite will automatically benchmark it and compare against baseline.
 * =============================================================================
 */

#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "../include/column_store.h"
#include "../include/csv_parser.h"
#include "../include/output_writer.h"
#include "../include/query_engine.h"

// =============================================================================
// Performance counters (global, reset before each config run)
// =============================================================================
namespace perf {
    uint64_t rows_scanned     = 0;  // total row iterations across all queries
    uint64_t town_comparisons = 0;  // town predicate comparisons (string or int)
    uint64_t rows_passed      = 0;  // rows that passed all filters
    uint64_t queries_valid    = 0;  // (x,y) pairs with a valid result

    void reset() {
        rows_scanned = 0;
        town_comparisons = 0;
        rows_passed = 0;
        queries_valid = 0;
    }
}

// =============================================================================
// Optimisation configuration
// =============================================================================
struct OptConfig {
    std::string name;           // display name for the table
    bool dict_encoding;         // A1
    bool reuse;                 // C1+C2
    bool precompute_ppsm;       // A4
    bool int_multiply;          // C6
    bool predicate_reorder;     // C4

    // apply this config to a ColumnStore before loading
    void apply(ColumnStore& db) const {
        db.use_dict_encoding     = dict_encoding;
        db.use_reuse             = reuse;
        db.use_precomputed_ppsm  = precompute_ppsm;
        db.use_int_multiply      = int_multiply;
        db.use_predicate_reorder = predicate_reorder;
    }
};

// =============================================================================
// Memory estimation
// =============================================================================
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

    bytes += stringColBytes(db.col_town);
    bytes += stringColBytes(db.col_block);
    bytes += stringColBytes(db.col_street_name);
    bytes += stringColBytes(db.col_flat_type);
    bytes += stringColBytes(db.col_flat_model);
    bytes += stringColBytes(db.col_storey_range);

    // encoded columns (if populated)
    bytes += db.col_town_encoded.capacity() * sizeof(uint16_t);
    bytes += db.col_flat_type_encoded.capacity() * sizeof(uint16_t);
    bytes += db.col_flat_model_encoded.capacity() * sizeof(uint16_t);
    bytes += db.col_street_name_encoded.capacity() * sizeof(uint16_t);
    // pre-computed PPSM column (A4)
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

    return bytes;
}

// =============================================================================
// Instrumented query runner (wraps runQuery with perf counters)
// =============================================================================
static void runQueryInstrumented(
    const ColumnStore&              db,
    int                             x,
    int                             y,
    uint16_t                        target_year,
    uint8_t                         start_month,
    const std::vector<std::string>& towns,
    QueryResult&                    result)
{
    runQuery(db, x, y, target_year, start_month, towns, result);

    if (db.use_reuse) {
        if (!result.no_result) ++perf::queries_valid;
        return;
    }

    // count rows scanned and town comparisons for this query
    const std::size_t N = db.size();
    const uint8_t end_month = static_cast<uint8_t>(
        std::min(static_cast<int>(start_month) + x - 1, 12));

    uint64_t local_rows = 0;
    uint64_t local_town_cmp = 0;
    uint64_t local_passed = 0;

    // pre-resolve town IDs if dict encoding is on
    std::vector<uint16_t> town_ids;
    if (db.use_dict_encoding) {
        for (const auto& t : towns) {
            uint16_t id;
            if (db.dict_town.lookup(t, id)) town_ids.push_back(id);
        }
    }

    for (std::size_t i = 0; i < N; ++i) {
        ++local_rows;

        if (db.use_predicate_reorder) {
            // C4 ON: Town → Year → Month
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
            if (db.col_month_year[i] != target_year) continue;
            if (db.col_month_month[i] < start_month || db.col_month_month[i] > end_month) continue;
        } else {
            // C4 OFF: Year → Month → Town
            if (db.col_month_year[i] != target_year) continue;
            if (db.col_month_month[i] < start_month || db.col_month_month[i] > end_month) continue;
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

        if (db.col_floor_area[i] < static_cast<uint16_t>(y)) continue;
        ++local_passed;
    }

    perf::rows_scanned += local_rows;
    perf::town_comparisons += local_town_cmp;
    perf::rows_passed += local_passed;
    if (!result.no_result) ++perf::queries_valid;
}

// =============================================================================
// Run all queries for a config; return results + timing
// =============================================================================
struct BenchmarkResult {
    std::string config_name;
    double      load_time_ms;
    double      query_time_ms;       // average across num_runs
    double      total_time_ms;       // load + query avg
    uint64_t    rows_scanned;
    uint64_t    town_comparisons;
    uint64_t    rows_passed;
    uint64_t    queries_valid;
    std::size_t memory_bytes;
    std::size_t dict_town_size;
    std::size_t dict_flat_type_size;
    std::size_t dict_flat_model_size;
    std::size_t dict_street_size;
    std::vector<QueryResult> results; // for correctness comparison
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

    // --- LOAD PHASE (timed once) ---
    ColumnStore db;
    config.apply(db);

    auto t_load_start = std::chrono::high_resolution_clock::now();
    loadCSV(csv_path, db);
    auto t_load_end = std::chrono::high_resolution_clock::now();

    bm.load_time_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    bm.memory_bytes = estimateMemoryBytes(db);

    // --- build cumulative table if reuse is enabled ---
    std::vector<std::vector<MinEntry>> cum_table;
    if (config.reuse) {
        db.cum_table = buildCumulativeTable(db, target_year, start_month, towns);;
    }

    // dictionary stats
    bm.dict_town_size      = db.dict_town.size();
    bm.dict_flat_type_size = db.dict_flat_type.size();
    bm.dict_flat_model_size = db.dict_flat_model.size();
    bm.dict_street_size    = db.dict_street_name.size();

    // --- QUERY PHASE (timed, averaged over num_runs) ---
    std::vector<double> run_times;
    run_times.reserve(num_runs);

    for (int run = 0; run < num_runs; ++run) {
        perf::reset();
        std::vector<QueryResult> results;
        results.reserve(8 * 71);

        auto t_q_start = std::chrono::high_resolution_clock::now();

        std::vector<std::vector<MinEntry>> run_cum_table;
        if (config.reuse) {
            db.cum_table = buildCumulativeTable(db, target_year, start_month, towns);
            // count the single scan that buildCumulativeTable performs
            perf::rows_scanned += db.size();
            // count town comparisons (1 hash lookup per row that passes year+month)
            for (std::size_t i = 0; i < db.size(); ++i) {
                if (db.col_month_year[i] != target_year) continue;
                int offset = static_cast<int>(db.col_month_month[i]) - static_cast<int>(start_month) + 1;
                if (offset < 1 || offset > 8) continue;
                ++perf::town_comparisons;  // one set lookup per surviving row
            }
        }

        for (int x = 1; x <= 8; ++x) {
            for (int y = 80; y <= 150; ++y) {
                QueryResult result;
                runQueryInstrumented(db, x, y, target_year, start_month, towns, result);
                results.push_back(result);
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
            bm.rows_passed      = perf::rows_passed;
            bm.queries_valid    = perf::queries_valid;
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

    bm.total_time_ms = bm.load_time_ms + bm.query_time_ms;
    return bm;
}

// =============================================================================
// Correctness check: compare results against baseline
// =============================================================================
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

// =============================================================================
// Pretty-print helpers
// =============================================================================
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

// =============================================================================
// Main
// =============================================================================
int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <path_to_csv> <MatriculationNumber> [num_runs=5]\n";
        return 1;
    }

    const std::string csv_path = argv[1];
    const std::string matric   = argv[2];
    const int num_runs = (argc >= 4) ? std::atoi(argv[3]) : 5;

    std::cout << "========================================\n";
    std::cout << "  HDB Column Store – Evaluation Suite\n";
    std::cout << "========================================\n";
    std::cout << "CSV file     : " << csv_path << "\n";
    std::cout << "Matric       : " << matric << "\n";
    std::cout << "Runs per cfg : " << num_runs << "\n\n";

    // =====================================================================
    // CONFIGURATION REGISTRY
    // Add new optimisation configs here. Each will be benchmarked and
    // compared against the first entry (baseline).
    // =====================================================================
    std::vector<OptConfig> configs = {
        //                                    A1     C1/C2  A4     C6     C4
        { "Baseline",                         false, false, false, false, false },
        { "A1: Dict Encoding",                true,  false, false, false, false },
        { "C1+C2: Result Reuse",              false, true,  false, false, false },
        { "A1+C1+C2: Dict+Reuse",             true,  true,  false, false, false },
        { "A4: Precompute PPSM",              false, false, true,  false, false },
        { "C6: Int Multiply",                 false, false, false, true,  false },
        { "C4: Predicate Reorder",            false, false, false, false, true  },
        { "A4+C6+C4: Precompute PPSM + Int Multiply + Predicate Reorder",        false, false, true,  true,  true  },
        { "A1+A4+C6+C4: Dict + Precompute PPSM + Int Multiply + Predicate Reorder",        true,  false, true,  true,  true  },
        { "A1+A4+C6+C4+C1C2: All",            true,  true,  true,  true,  true  },
    };

    // =====================================================================
    // Run all configs
    // =====================================================================
    std::vector<BenchmarkResult> all_results;
    all_results.reserve(configs.size());

    for (const auto& cfg : configs) {
        std::cout << "--- Running: " << cfg.name << " ---\n";
        auto bm = runBenchmark(csv_path, matric, cfg, num_runs);
        all_results.push_back(std::move(bm));
        std::cout << "\n";
    }

    // =====================================================================
    // Correctness check (all configs vs baseline)
    // =====================================================================
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

    // =====================================================================
    // Performance comparison table
    // =====================================================================
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

    // =====================================================================
    // Dictionary stats (if any config uses it)
    // =====================================================================
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

    // =====================================================================
    // Detailed delta analysis (each config vs baseline)
    // =====================================================================
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

    return all_correct ? 0 : 1;
}