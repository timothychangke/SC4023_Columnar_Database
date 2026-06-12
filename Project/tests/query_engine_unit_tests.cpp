#include <cmath>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "../include/column_file_io.h"
#include "../include/query_engine.h"

namespace {

int g_assertions_run = 0;
int g_assertions_failed = 0;
int g_tests_run = 0;
int g_tests_failed = 0;
bool g_current_test_failed = false;

#define CHECK(cond, msg)                                                          \
    do {                                                                           \
        ++g_assertions_run;                                                        \
        if (!(cond)) {                                                             \
            std::cerr << "    [FAIL] " << msg << "\n";                           \
            ++g_assertions_failed;                                                 \
            g_current_test_failed = true;                                          \
        } else {                                                                   \
            std::cout << "    [OK] " << msg << "\n";                             \
        }                                                                          \
    } while (0)

#define RUN_TEST(fn)                                                               \
    do {                                                                           \
        ++g_tests_run;                                                             \
        g_current_test_failed = false;                                             \
        std::cout << "[RUN ] " << #fn << "\n";                                   \
        fn();                                                                      \
        if (g_current_test_failed) {                                               \
            ++g_tests_failed;                                                      \
            std::cout << "[FAIL] " << #fn << "\n\n";                             \
        } else {                                                                   \
            std::cout << "[PASS] " << #fn << "\n\n";                             \
        }                                                                          \
    } while (0)

bool approx(double a, double b, double eps = 1e-9) {
    return std::fabs(a - b) <= eps;
}

ColumnStore makeFixture(bool dict_encoding, bool precompute_ppsm) {
    ColumnStore db;
    db.use_dict_encoding = dict_encoding;
    db.use_precomputed_ppsm = precompute_ppsm;

    auto push_row = [&](uint16_t year,
                        uint8_t month,
                        const std::string& town,
                        const std::string& block,
                        uint16_t area,
                        const std::string& flat_model,
                        uint16_t lease,
                        uint32_t price) {
        db.col_month_year.push_back(year);
        db.col_month_month.push_back(month);
        db.col_town.push_back(town);
        db.col_block.push_back(block);
        db.col_floor_area.push_back(area);
        db.col_flat_model.push_back(flat_model);
        db.col_lease_commence_date.push_back(lease);
        db.col_resale_price.push_back(price);

        db.col_street_name.push_back("STREET");
        db.col_flat_type.push_back("TYPE");
        db.col_storey_range.push_back("01 TO 03");

        if (dict_encoding) {
            db.col_town_encoded.push_back(db.dict_town.encode(town));
            db.col_street_name_encoded.push_back(db.dict_street_name.encode("STREET"));
            db.col_flat_type_encoded.push_back(db.dict_flat_type.encode("TYPE"));
            db.col_flat_model_encoded.push_back(db.dict_flat_model.encode(flat_model));
        }

        if (precompute_ppsm) {
            db.col_price_per_sqm.push_back(static_cast<double>(price) / static_cast<double>(area));
        }
    };

    // row 0: valid, BEDOK, ppsm = 3000
    push_row(2016, 3, "BEDOK", "A", 90, "Model A", 1990, 270000);
    // row 1: valid, BEDOK, ppsm = 2800 (better when x >= 2)
    push_row(2016, 4, "BEDOK", "B", 100, "Model B", 1991, 280000);
    // row 2: target-year/town but above 4725 threshold => invalid final answer
    push_row(2016, 3, "CLEMENTI", "C", 85, "Model C", 1992, 500000);
    // row 3: different year
    push_row(2017, 3, "BEDOK", "D", 120, "Model D", 1993, 200000);

    return db;
}

ColumnStore makeSortedFixture(bool dict_encoding, bool precompute_ppsm) {
    ColumnStore db;
    db.use_dict_encoding = dict_encoding;
    db.use_precomputed_ppsm = precompute_ppsm;

    auto push_row = [&](uint16_t year,
                        uint8_t month,
                        const std::string& town,
                        const std::string& block,
                        uint16_t area,
                        const std::string& flat_model,
                        uint16_t lease,
                        uint32_t price) {
        db.col_month_year.push_back(year);
        db.col_month_month.push_back(month);
        db.col_town.push_back(town);
        db.col_block.push_back(block);
        db.col_floor_area.push_back(area);
        db.col_flat_model.push_back(flat_model);
        db.col_lease_commence_date.push_back(lease);
        db.col_resale_price.push_back(price);

        db.col_street_name.push_back("STREET");
        db.col_flat_type.push_back("TYPE");
        db.col_storey_range.push_back("01 TO 03");

        if (dict_encoding) {
            db.col_town_encoded.push_back(db.dict_town.encode(town));
            db.col_street_name_encoded.push_back(db.dict_street_name.encode("STREET"));
            db.col_flat_type_encoded.push_back(db.dict_flat_type.encode("TYPE"));
            db.col_flat_model_encoded.push_back(db.dict_flat_model.encode(flat_model));
        }

        if (precompute_ppsm) {
            db.col_price_per_sqm.push_back(static_cast<double>(price) / static_cast<double>(area));
        }
    };

    // Keep towns contiguous to emulate pre-sorted partitions.
    push_row(2016, 3, "BEDOK", "A", 90, "Model A", 1990, 270000);   // 3000
    push_row(2016, 4, "BEDOK", "B", 100, "Model B", 1991, 280000);  // 2800
    push_row(2017, 3, "BEDOK", "D", 120, "Model D", 1993, 200000);  // other year
    push_row(2016, 3, "CLEMENTI", "C", 85, "Model C", 1992, 500000); // >4725

    return db;
}

void buildPartitionsFromCurrentOrder(ColumnStore& db) {
    db.town_partitions.clear();
    db.town_partitions_encoded.clear();

    const std::size_t N = db.size();
    if (N == 0) return;

    if (db.use_dict_encoding) {
        db.town_partitions_encoded.resize(db.dict_town.size());
        std::size_t i = 0;
        while (i < N) {
            const uint16_t tid = db.col_town_encoded[i];
            const std::size_t begin = i;
            while (i < N && db.col_town_encoded[i] == tid) ++i;
            const std::size_t end = i;
            if (tid < db.town_partitions_encoded.size()) {
                db.town_partitions_encoded[tid] = TownPartition{begin, end, true};
                db.town_partitions[db.dict_town.decode(tid)] = TownPartition{begin, end, true};
            }
        }
    } else {
        std::size_t i = 0;
        while (i < N) {
            const std::string town = db.col_town[i];
            const std::size_t begin = i;
            while (i < N && db.col_town[i] == town) ++i;
            const std::size_t end = i;
            db.town_partitions[town] = TownPartition{begin, end, true};
        }
    }
}

void test_buildTownList() {
    const auto towns = buildTownList("U2220136J");
    const std::vector<std::string> expected = {
        "CLEMENTI", "BEDOK", "BUKIT PANJANG", "CHOA CHU KANG", "PASIR RIS"
    };
    CHECK(towns == expected, "buildTownList should preserve first-seen digit order");
}

void test_deriveQueryParams() {
    uint16_t year = 0;
    uint8_t month = 0;
    deriveQueryParams("U2220136J", year, month);
    CHECK(year == 2016, "deriveQueryParams year should map to 2016");
    CHECK(month == 3, "deriveQueryParams start month should be 3");

    bool threw = false;
    try {
        deriveQueryParams("ABCD", year, month);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw, "deriveQueryParams should throw when matric has fewer than 2 digits");
}

void test_buildTownBitmapMask() {
    ColumnStore db = makeFixture(true, false);
    db.use_bitmap_index_town = true;
    db.rebuildTownBitmaps();

    const auto mask = buildTownBitmapMask(db, {"BEDOK"});
    CHECK(mask.size() == db.size(), "buildTownBitmapMask should return one entry per row");
    CHECK(mask[0] == 1 && mask[1] == 1 && mask[2] == 0 && mask[3] == 1,
          "buildTownBitmapMask BEDOK mask values should match encoded town rows");
}

void test_buildTownBitmapMask_stringPath() {
    ColumnStore db = makeFixture(false, false);
    db.use_bitmap_index_town = true;
    db.rebuildTownBitmaps();

    const auto mask = buildTownBitmapMask(db, {"CLEMENTI"});
    CHECK(mask.size() == db.size(), "string-path bitmap mask should return one entry per row");
    CHECK(mask[0] == 0 && mask[1] == 0 && mask[2] == 1 && mask[3] == 0,
          "string-path bitmap should isolate CLEMENTI row only");
}

void test_runQuery_and_buildCumulativeTable() {
    ColumnStore db = makeFixture(true, true);

    QueryResult r;
    runQuery(db, 1, 80, 2016, 3, {"BEDOK"}, r, nullptr);
    CHECK(!r.no_result, "runQuery should find a valid result for BEDOK, x=1, y=80");
    CHECK(r.year == 2016 && r.month == 3, "runQuery should return row from Mar 2016");
    CHECK(r.town == "BEDOK" && r.block == "A", "runQuery should materialize BEDOK row A");
    CHECK(approx(r.price_per_sqm, 3000.0), "runQuery should compute PPSM = 3000");

    QueryResult r_no;
    runQuery(db, 1, 80, 2016, 3, {"CLEMENTI"}, r_no, nullptr);
    CHECK(r_no.no_result, "runQuery should mark no_result when best PPSM exceeds 4725");

    const auto cum = buildCumulativeTable(db, 2016, 3, {"BEDOK"});
    CHECK(cum.size() == 9 && cum[1].size() == 151,
          "buildCumulativeTable should build [9][151] table");
    CHECK(cum[1][80].has && approx(cum[1][80].ppsm, 3000.0),
          "buildCumulativeTable x=1,y=80 should be 3000");
    CHECK(cum[2][80].has && approx(cum[2][80].ppsm, 2800.0),
          "buildCumulativeTable x=2,y=80 should improve to 2800");

    db.use_reuse = true;
    db.cum_table = cum;
    QueryResult r_reuse;
    runQuery(db, 2, 80, 2016, 3, {"BEDOK"}, r_reuse, nullptr);
    CHECK(!r_reuse.no_result && approx(r_reuse.price_per_sqm, 2800.0),
          "runQuery reuse path should answer from cumulative table");
}

void test_runAllQueriesChunked() {
    ColumnStore source = makeFixture(true, true);
    source.use_dict_encoding = true;
    source.use_precomputed_ppsm = true;

    const std::string dir = "data/test_columns_query_engine_unit";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    writeColumnFiles(source, dir);

    ColumnStore base = source;
    base.use_columnar_files = true;
    base.column_dir = dir;
    base.total_rows = source.size();
    base.io_chunk_rows = 2;
    base.use_zone_maps = false;
    base.use_bitmap_index_town = false;
    base.use_rle_town = false;
    base.use_late_materialise = false;
    base.use_int_multiply = false;
    base.use_predicate_reorder = false;

    std::vector<QueryResult> results;
    runAllQueriesChunked(base, 2016, 3, {"BEDOK"}, results);

    CHECK(results.size() == 8 * 71, "runAllQueriesChunked should produce 568 slots");

    const QueryResult& slot_x1_y80 = results[0]; // (1,80)
    CHECK(!slot_x1_y80.no_result, "runAllQueriesChunked slot (1,80) should have result");
    CHECK(slot_x1_y80.town == "BEDOK", "runAllQueriesChunked should materialize BEDOK");
    CHECK(approx(slot_x1_y80.price_per_sqm, 3000.0),
          "runAllQueriesChunked (1,80) PPSM should be 3000");

    std::filesystem::remove_all(dir);
}

void test_runQuery_presorted_binary_search_path() {
    ColumnStore db = makeSortedFixture(true, true);
    db.use_presorted_storage = true;
    db.use_month_binary_search = true;
    buildPartitionsFromCurrentOrder(db);

    QueryResult r;
    runQuery(db, 2, 80, 2016, 3, {"BEDOK"}, r, nullptr);
    CHECK(!r.no_result, "presorted+bsearch path should return result");
    CHECK(approx(r.price_per_sqm, 2800.0), "presorted+bsearch should pick lower PPSM in month window");
    CHECK(r.block == "B", "presorted+bsearch should materialize winning row B");
}

void test_runQuery_rle_path() {
    ColumnStore db = makeSortedFixture(true, true);
    db.use_rle_town = true;
    db.buildTownRLE();

    QueryResult r;
    runQuery(db, 2, 80, 2016, 3, {"BEDOK"}, r, nullptr);
    CHECK(!r.no_result, "RLE path should return result");
    CHECK(approx(r.price_per_sqm, 2800.0), "RLE path should preserve min PPSM semantics");
    CHECK(db.town_runs_scanned > 0, "RLE path should scan at least one run");
}

void test_runQuery_unified_scan_bitmap_predicate_reorder_zone_map() {
    ColumnStore db = makeFixture(true, true);
    db.use_bitmap_index_town = true;
    db.rebuildTownBitmaps();

    db.use_zone_maps = true;
    db.zm_floor_area.chunks = {{80, 150}};
    db.zm_month_year.chunks = {{2016, 2017}};
    db.zm_month_month.chunks = {{3, 4}};

    db.use_predicate_reorder = true;
    db.use_int_multiply = true;

    const auto mask = buildTownBitmapMask(db, {"BEDOK"});

    QueryResult r;
    runQuery(db, 2, 80, 2016, 3, {"BEDOK"}, r, &mask);
    CHECK(!r.no_result, "unified scan path should return result with bitmap+zone map+predicate reorder");
    CHECK(approx(r.price_per_sqm, 2800.0), "unified scan path should preserve min PPSM result");
}

void test_runQuery_late_materialise_path() {
    ColumnStore db = makeFixture(true, true);
    db.use_late_materialise = true;

    QueryResult r;
    runQuery(db, 2, 80, 2016, 3, {"BEDOK"}, r, nullptr);
    CHECK(!r.no_result, "late materialise path should return result");
    CHECK(approx(r.price_per_sqm, 2800.0), "late materialise should preserve min PPSM");
}

} // namespace

int main() {
    RUN_TEST(test_buildTownList);
    RUN_TEST(test_deriveQueryParams);
    RUN_TEST(test_buildTownBitmapMask);
    RUN_TEST(test_buildTownBitmapMask_stringPath);
    RUN_TEST(test_runQuery_and_buildCumulativeTable);
    RUN_TEST(test_runQuery_presorted_binary_search_path);
    RUN_TEST(test_runQuery_rle_path);
    RUN_TEST(test_runQuery_unified_scan_bitmap_predicate_reorder_zone_map);
    RUN_TEST(test_runQuery_late_materialise_path);
    RUN_TEST(test_runAllQueriesChunked);

    std::cout << "========== query_engine unit test summary ==========" << "\n";
    std::cout << "Tests run:        " << g_tests_run << "\n";
    std::cout << "Tests failed:     " << g_tests_failed << "\n";
    std::cout << "Assertions run:   " << g_assertions_run << "\n";
    std::cout << "Assertions failed:" << g_assertions_failed << "\n";

    if (g_tests_failed == 0 && g_assertions_failed == 0) {
        std::cout << "[PASS] query_engine unit test suite\n";
        return 0;
    }

    std::cerr << "[FAIL] query_engine unit test suite\n";
    return 1;
}
