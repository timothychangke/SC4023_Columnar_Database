/*
 * main entry point for the SC4023 big data project.
 * this file is intentionally kept thin. it just orchestrates the main flow:
 * 1. parse the matriculation number from cmd args
 * 2. figure out the query params from it
 * 3. ingest ResalePricesSingapore.csv into our column store
 * 4. run all the (x, y) queries and write to csv
 *
 * Usage:
 * ./column_store <MatriculationNumber> [--dict-encoding] [--reuse]
 * eg ./column_store U1234567A
 * eg ./column_store U1234567A --dict-encoding
 * eg ./column_store U1234567A --reuse
 *
 *
 * Optimisation flags:
 * --dict-encoding   Enable dictionary encoding (A1) for string columns.
 *                   Replaces string comparisons with int comparisons during queries.
 * --presort-storage Enable pre-sorted storage (A2): sort by Town->Year->Month.
 * --month-bsearch   Enable month binary search index (B3).
 *                   Most effective when used with --presort-storage.
 * --reuse           Enables intermediate result reuse (C1 and C2).
 *                   Prevent rescanning full tables for different (x, y) when possible.
 */

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "column_store.h"
#include "csv_parser.h"
#include "output_writer.h"
#include "query_engine.h"
#include "column_file_io.h"

int main(int argc, char* argv[]) {

    // phase 0: check command line args
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <MatriculationNumber> [flags...]\n"
        << "Flags: --dict-encoding --bitmap-index-town --presort-storage --month-bsearch --reuse\n"
        << "       --precompute-ppsm --int-multiply --predicate-reorder --zone-maps --late-materialise\n";
        << "       --mmap-io --town-partitioning --write-columns-partitioned\n";
        std::cerr << "Example: " << argv[0] << " A5656567B\n";
        std::cerr << "Example: " << argv[0] << " A5656567B --dict-encoding\n";

        return 1;
    }
    const std::string matric_number = argv[1];
    std::cout << "Matriculation number : " << matric_number << "\n";

    // parse optimisation flags from command line
    bool enable_dict_encoding = false;
    bool enable_reuse = false;
    bool enable_precomputed_ppsm   = false;
    bool enable_int_multiply       = false;
    bool enable_predicate_reorder  = false;
    bool enable_zone_maps = false;
    bool enable_bitmap_index_town = false;
    bool enable_presorted_storage = false;
    bool enable_month_bsearch = false;
    bool enable_late_materialise = false;
    bool enable_columnar_files = false;
    bool enable_mmap_io = false;
    bool enable_town_partitioning = false;
    bool write_columns_mode = false;
    bool write_columns_partitioned_mode = false;

    for (int i = 2; i < argc; ++i) {
        if (std::strcmp(argv[i], "--dict-encoding") == 0) {
            enable_dict_encoding = true;
        }
        if (std::strcmp(argv[i], "--reuse") == 0) {
            enable_reuse = true;
        }
        if (std::strcmp(argv[i], "--precompute-ppsm") == 0) {
            enable_precomputed_ppsm = true;
        }
        if (std::strcmp(argv[i], "--int-multiply") == 0) {
            enable_int_multiply = true;
        }
        if (std::strcmp(argv[i], "--predicate-reorder") == 0) {
            enable_predicate_reorder = true;
        }
        if (std::strcmp(argv[i], "--zone-maps") == 0) {
            enable_zone_maps = true;
        }
        if (std::strcmp(argv[i], "--bitmap-index-town") == 0) {
            enable_bitmap_index_town = true;
        }
        if (std::strcmp(argv[i], "--presort-storage") == 0) {
            enable_presorted_storage = true;
        }
        if (std::strcmp(argv[i], "--month-bsearch") == 0) {
            enable_month_bsearch = true;
        }
        if (std::strcmp(argv[i], "--late-materialise") == 0) {
            enable_late_materialise = true;
        }
        if (std::strcmp(argv[i], "--columnar-files") == 0) {
            enable_columnar_files = true;
        }
        if (std::strcmp(argv[i], "--mmap-io") == 0) {
            enable_mmap_io = true;
        }
        if (std::strcmp(argv[i], "--write-columns") == 0) {
            write_columns_mode = true;
        }
        if (std::strcmp(argv[i], "--town-partitioning") == 0) {
            enable_town_partitioning = true;
        }
        if (std::strcmp(argv[i], "--write-columns-partitioned") == 0) {
            write_columns_partitioned_mode = true;
        }
        // add more flags here as we implement more optimisations
    }

    std::cout << "--- Optimisation Flags ---\n";
    std::cout << "  Dictionary Encoding:        "
              << (enable_dict_encoding ? "ON" : "OFF") << "\n";
    std::cout << "  Intermediate Result Reuse: "
              << (enable_reuse ? "ON" : "OFF") << "\n";
    std::cout << "  Precomputed PPSM:                "
              << (enable_precomputed_ppsm ? "ON" : "OFF") << "\n";
    std::cout << "  Integer Multiplication:          "
              << (enable_int_multiply ? "ON" : "OFF") << "\n";
    std::cout << "  Predicate Reordering:            "
              << (enable_predicate_reorder ? "ON" : "OFF") << "\n";
    std::cout << "  Zone Maps: "
          << (enable_zone_maps ? "ON" : "OFF") << "\n";
        std::cout << "  Bitmap Index (Town): "
            << (enable_bitmap_index_town ? "ON" : "OFF") << "\n";
        std::cout << "  Pre-sorted Storage (A2):       "
            << (enable_presorted_storage ? "ON" : "OFF") << "\n";
        std::cout << "  Month Binary Search (B3):      "
            << (enable_month_bsearch ? "ON" : "OFF") << "\n";
    std::cout << "  Late Materialisation:            "
              << (enable_late_materialise ? "ON" : "OFF") << "\n";
    std::cout << "  Columnar Files:              "
          << (enable_columnar_files ? "ON" : "OFF") << "\n";
    std::cout << "  Memory-Mapped I/O (D2):      "
          << (enable_mmap_io ? "ON" : "OFF") << "\n";
    std::cout << "  Town Partitioning (E1):      "
          << (enable_town_partitioning ? "ON" : "OFF") << "\n";
    std::cout << "--------------------------\n";
    
    // phase 1: extract query params from matric number
    uint16_t target_year = 0;
    uint8_t  start_month = 0;

    try {
        deriveQueryParams(matric_number, target_year, start_month);
    } catch (const std::invalid_argument& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    const std::vector<std::string> towns = buildTownList(matric_number);

    std::cout << "Target year  : " << target_year << "\n";
    std::cout << "Start month  : " << static_cast<int>(start_month) << "\n";
    std::cout << "Target towns : ";
    for (std::size_t i = 0; i < towns.size(); ++i) {
        std::cout << towns[i];
        if (i + 1 < towns.size()) std::cout << ", ";
    }
    std::cout << "\n";

    // phase 2: ingest dataset
    ColumnStore db;

    // toggle optimisations before loading
    db.use_dict_encoding = enable_dict_encoding;
    db.use_reuse = enable_reuse;
    db.use_precomputed_ppsm  = enable_precomputed_ppsm;
    db.use_int_multiply      = enable_int_multiply;
    db.use_predicate_reorder = enable_predicate_reorder;
    db.use_zone_maps = enable_zone_maps;
    db.use_bitmap_index_town = enable_bitmap_index_town;
    db.use_presorted_storage = enable_presorted_storage;
    db.use_month_binary_search = enable_month_bsearch;
    db.use_late_materialise = enable_late_materialise;
    db.use_columnar_files = enable_columnar_files;
    db.use_mmap_io = enable_mmap_io;
    db.use_town_partitioning = enable_town_partitioning;

    // One-time conversion mode: parse CSV, write column files, then exit
    if (write_columns_mode) {
        try {
            loadCSV("../data/ResalePricesSingapore.csv", db);
        } catch (const std::runtime_error& e) {
            std::cerr << "Error: " << e.what() << "\n";
            return 1;
        }
        writeColumnFiles(db, "data/columns/");
        std::cout << "Column files written. Now run with --columnar-files.\n";
        return 0;
    }
    // E1: One-time partitioned conversion mode
    if (write_columns_partitioned_mode) {
        try {
            loadCSV("../data/ResalePricesSingapore.csv", db);
        } catch (const std::runtime_error& e) {
            std::cerr << "Error: " << e.what() << "\n";
            return 1;
        }
        writeColumnFilesPartitioned(db, "data/columns_e1/");
        std::cout << "Partitioned column files written. Now run with --town-partitioning.\n";
        return 0;
    }

    try {
        // D2 validation: mmap requires columnar files, conflicts with chunked I/O
        if (db.use_mmap_io && !db.use_columnar_files) {
            std::cout << "  [D2] WARNING: mmap_io requires columnar_files — disabling D2.\n";
            db.use_mmap_io = false;
        }
        if (db.use_mmap_io && db.use_chunked_io) {
            std::cout << "  [D2] WARNING: mmap_io conflicts with chunked_io (D1) — disabling D2.\n";
            db.use_mmap_io = false;
        }

        try {
            if (db.use_town_partitioning) {
                // E1 validation
                if (!db.use_columnar_files) {
                    std::cout << "  [E1] NOTE: town_partitioning implies columnar_files, enabling.\n";
                    db.use_columnar_files = true;
                }
                loadColumnFilesPartitioned("data/columns_e1", towns, db);
            } else if (db.use_columnar_files && db.use_mmap_io) {
                loadColumnFilesMmap(db.column_dir, db);
            } else if (db.use_columnar_files) {
                loadColumnFiles(db.column_dir, db);
        } else {
            loadCSV("../data/ResalePricesSingapore.csv", db);
        }
    } catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    std::cout << "Total records in column store: " << db.size() << "\n";

    // phase 3: run all the (x, y) combinations
    std::vector<QueryResult> all_results;
    all_results.reserve(8 * 71); 

    std::vector<uint8_t> town_bitmap_mask;
    const std::vector<uint8_t>* town_bitmap_mask_ptr = nullptr;
    if (db.use_bitmap_index_town) {
        town_bitmap_mask = buildTownBitmapMask(db, towns);
        town_bitmap_mask_ptr = &town_bitmap_mask;
    }

    // if opted for optimisation 2 (reuse)
    if (!db.use_dict_encoding && db.use_reuse) {
        std::cout << "Building intermediate cumulative table for reuse...\n";
        db.cum_table = buildCumulativeTable(db, target_year, start_month, towns);
        std::cout << "Cumulative table built. Running queries using IR reuse...\n";
    } 

    for (int x = 1; x <= 8; ++x) {
        for (int y = 80; y <= 150; ++y) {
            QueryResult result;
            runQuery(db, x, y, target_year, start_month, towns, result, town_bitmap_mask_ptr);
            all_results.push_back(result);
        }
    }

    // phase 4: write results to output csv
    try {
        writeResults(all_results, matric_number);
    } catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    std::cout << "Done.\n";
    return 0;
}