/**
 * =============================================================================
 * test_suite.cpp
 * =============================================================================
 * Comprehensive test suite for the HDB Column Store project (baseline).
 *
 * Covers:
 *   1.  ColumnStore – storage, size, clear
 *   2.  trim() – whitespace handling
 *   3.  parseCSVLine() – delimiter, quoting, edge cases
 *   4.  parseMonthField() – valid formats, all months, year mapping, errors
 *   5.  loadCSV() – good data, bad file, missing columns, skipped rows
 *   6.  buildTownList() – digit-to-town mapping, deduplication, ordering
 *   7.  deriveQueryParams() – year/month derivation, edge cases, errors
 *   8.  runQuery() – predicates, PPSM computation, 4725 threshold, no_result
 *   9.  writeResults() – CSV format, month zero-pad, rounding, no_result skip
 *  10.  Integration – end-to-end pipeline on a known mini-dataset
 * =============================================================================
 */

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// headers are located in the top‑level include/ directory
#include "../include/column_store.h"
#include "../include/csv_parser.h"
#include "../include/output_writer.h"
#include "../include/query_engine.h"

// =============================================================================
// Minimal test harness
// =============================================================================

struct TestRunner
{
  int passed = 0;
  int failed = 0;

  void run(const std::string &name, std::function<void()> fn)
  {
    try
    {
      fn();
      std::cout << "  [PASS] " << name << "\n";
      ++passed;
    }
    catch (const std::exception &e)
    {
      std::cout << "  [FAIL] " << name << "\n"
                << "         Exception: " << e.what() << "\n";
      ++failed;
    }
    catch (...)
    {
      std::cout << "  [FAIL] " << name << "\n"
                << "         Unknown exception\n";
      ++failed;
    }
  }

  void section(const std::string &title)
  {
    std::cout << "\n=== " << title << " ===\n";
  }

  void summary() const
  {
    std::cout << "\n--------------------------------------------\n"
              << "  Results : " << passed << " passed, "
              << failed << " failed\n"
              << "--------------------------------------------\n";
    if (failed > 0)
      std::exit(1);
  }
};

// Helper: throw-based assert so failures propagate as exceptions
#define ASSERT(cond)                                  \
  do                                                  \
  {                                                   \
    if (!(cond))                                      \
      throw std::runtime_error(                       \
          std::string("Assertion failed: ") + #cond + \
          " at line " + std::to_string(__LINE__));    \
  } while (false)

#define ASSERT_EQ(a, b)                                      \
  do                                                         \
  {                                                          \
    if ((a) != (b))                                          \
    {                                                        \
      std::ostringstream _oss;                               \
      _oss << "Expected [" << (b) << "] got [" << (a) << "]" \
           << " at line " << __LINE__;                       \
      throw std::runtime_error(_oss.str());                  \
    }                                                        \
  } while (false)

#define ASSERT_NEAR(a, b, eps)                                \
  do                                                          \
  {                                                           \
    if (std::fabs((double)(a) - (double)(b)) > (double)(eps)) \
    {                                                         \
      std::ostringstream _oss;                                \
      _oss << "Values not near enough: " << (a)               \
           << " vs " << (b) << " (eps=" << (eps) << ")"       \
           << " at line " << __LINE__;                        \
      throw std::runtime_error(_oss.str());                   \
    }                                                         \
  } while (false)

#define ASSERT_THROWS(expr)                                                                                 \
  do                                                                                                        \
  {                                                                                                         \
    bool _threw = false;                                                                                    \
    try                                                                                                     \
    {                                                                                                       \
      (expr);                                                                                               \
    }                                                                                                       \
    catch (...)                                                                                             \
    {                                                                                                       \
      _threw = true;                                                                                        \
    }                                                                                                       \
    if (!_threw)                                                                                            \
      throw std::runtime_error(                                                                             \
          std::string("Expected exception not thrown: ") + #expr + " at line " + std::to_string(__LINE__)); \
  } while (false)

// =============================================================================
// CSV fixture helpers
// =============================================================================

/** Write a temporary CSV file; returns its filename. */
static std::string writeTmpCSV(const std::string &content,
                               const std::string &name = "tmp_test.csv")
{
  std::ofstream f(name);
  f << content;
  f.close();
  return name;
}

/** The canonical 10-column header used by the sample dataset. */
static const std::string HEADER_10 =
    "month,town,flat_type,block,street_name,storey_range,"
    "floor_area_sqm,flat_model,lease_commence_date,resale_price\n";

/** The 11-column header (with remaining_lease) used in the real dataset. */
static const std::string HEADER_11 =
    "month,town,flat_type,block,street_name,storey_range,"
    "floor_area_sqm,flat_model,lease_commence_date,remaining_lease,resale_price\n";

/** A single valid data row for the 10-col layout. */
static const std::string ROW_VALID_10 =
    "Jan-15,TAMPINES,5 ROOM,274,TAMPINES ST 22,10 TO 12,105,New Generation,1985,404000\n";

/** Same record but for the 11-col layout (adds remaining_lease). */
static const std::string ROW_VALID_11 =
    "Jan-15,TAMPINES,5 ROOM,274,TAMPINES ST 22,10 TO 12,105,New Generation,1985,61 years,404000\n";

// =============================================================================
// Helper: build a ColumnStore with a single known record
// =============================================================================
static ColumnStore makeSingleRecordDB(
    uint16_t year, uint8_t month,
    const std::string &town,
    uint16_t floor_area, uint32_t resale_price,
    const std::string &block = "1",
    const std::string &flat_model = "Improved",
    uint16_t lease_cd = 1990,
    bool dict_encoding = false)
{
  ColumnStore db;
  db.use_dict_encoding = dict_encoding;

  db.col_month_year.push_back(year);
  db.col_month_month.push_back(month);
  db.col_town.push_back(town);
  db.col_block.push_back(block);
  db.col_street_name.push_back("TEST ST");
  db.col_flat_type.push_back("3 ROOM");
  db.col_flat_model.push_back(flat_model);
  db.col_storey_range.push_back("01 TO 03");
  db.col_floor_area.push_back(floor_area);
  db.col_lease_commence_date.push_back(lease_cd);
  db.col_resale_price.push_back(resale_price);

  if (dict_encoding) {
    db.col_town_encoded.push_back(db.dict_town.encode(town));
    db.col_flat_type_encoded.push_back(db.dict_flat_type.encode("3 ROOM"));
    db.col_flat_model_encoded.push_back(db.dict_flat_model.encode(flat_model));
    db.col_street_name_encoded.push_back(db.dict_street_name.encode("TEST ST"));
  }

  return db;
}

// =============================================================================
// Section 1: ColumnStore
// =============================================================================
static void testColumnStore(TestRunner &t)
{
  t.section("1. ColumnStore – storage, size, clear");

  t.run("Empty store has size 0", []()
        {
        ColumnStore db;
        ASSERT_EQ(db.size(), 0u); });

  t.run("Size reflects number of records pushed", []()
        {
        ColumnStore db;
        db.col_month_year.push_back(2020);
        db.col_month_month.push_back(3);
        db.col_town.push_back("TAMPINES");
        db.col_block.push_back("10");
        db.col_street_name.push_back("ST");
        db.col_flat_type.push_back("3 ROOM");
        db.col_flat_model.push_back("Improved");
        db.col_storey_range.push_back("01 TO 03");
        db.col_floor_area.push_back(100);
        db.col_lease_commence_date.push_back(1990);
        db.col_resale_price.push_back(350000);
        ASSERT_EQ(db.size(), 1u); });

  t.run("Clear resets all columns to zero size", []()
        {
        ColumnStore db = makeSingleRecordDB(2018, 5, "TAMPINES", 100, 300000);
        ASSERT_EQ(db.size(), 1u);
        db.clear();
        ASSERT_EQ(db.size(), 0u);
        ASSERT(db.col_month_year.empty());
        ASSERT(db.col_town.empty());
        ASSERT(db.col_resale_price.empty()); });

  t.run("All column vectors stay in sync (parallel arrays)", []()
        {
        ColumnStore db;
        for (int i = 0; i < 5; ++i) {
            db.col_month_year.push_back(static_cast<uint16_t>(2015 + i));
            db.col_month_month.push_back(static_cast<uint8_t>(i + 1));
            db.col_town.push_back("BEDOK");
            db.col_block.push_back(std::to_string(i));
            db.col_street_name.push_back("ST");
            db.col_flat_type.push_back("4 ROOM");
            db.col_flat_model.push_back("Standard");
            db.col_storey_range.push_back("01 TO 03");
            db.col_floor_area.push_back(static_cast<uint16_t>(80 + i));
            db.col_lease_commence_date.push_back(1980);
            db.col_resale_price.push_back(300000u + i * 10000u);
        }
        ASSERT_EQ(db.col_month_year.size(),       db.size());
        ASSERT_EQ(db.col_town.size(),              db.size());
        ASSERT_EQ(db.col_floor_area.size(),        db.size());
        ASSERT_EQ(db.col_resale_price.size(),      db.size()); });

  t.run("Correct data types stored without overflow (uint32 price)", []()
        {
        ColumnStore db;
        db.col_resale_price.push_back(1200000u);  // large but valid price
        ASSERT_EQ(db.col_resale_price[0], 1200000u); });
}

// =============================================================================
// Section 2: trim()
// =============================================================================
static void testTrim(TestRunner &t)
{
  t.section("2. trim() – whitespace handling");

  t.run("No whitespace – unchanged", []()
        { ASSERT_EQ(trim("TAMPINES"), "TAMPINES"); });
  t.run("Leading spaces stripped", []()
        { ASSERT_EQ(trim("   hello"), "hello"); });
  t.run("Trailing spaces stripped", []()
        { ASSERT_EQ(trim("hello   "), "hello"); });
  t.run("Leading and trailing stripped", []()
        { ASSERT_EQ(trim("  hello world  "), "hello world"); });
  t.run("Tabs and newlines stripped", []()
        { ASSERT_EQ(trim("\t\r\nhello\r\n"), "hello"); });
  t.run("All-whitespace string returns empty", []()
        { ASSERT_EQ(trim("    "), ""); });
  t.run("Empty string returns empty", []()
        { ASSERT_EQ(trim(""), ""); });
  t.run("Internal whitespace preserved", []()
        { ASSERT_EQ(trim("  ANG MO KIO  "), "ANG MO KIO"); });
}

// =============================================================================
// Section 3: parseCSVLine()
// =============================================================================
static void testParseCSVLine(TestRunner &t)
{
  t.section("3. parseCSVLine() – delimiter, quoting, edge cases");

  t.run("Simple 3-field line splits correctly", []()
        {
        std::vector<std::string> fields;
        parseCSVLine("Jan-15,TAMPINES,104000", fields);
        ASSERT_EQ(fields.size(), 3u);
        ASSERT_EQ(fields[0], "Jan-15");
        ASSERT_EQ(fields[1], "TAMPINES");
        ASSERT_EQ(fields[2], "104000"); });

  t.run("Full 10-column data row parses all fields", []()
        {
        std::vector<std::string> fields;
        parseCSVLine(
            "Jan-15,TAMPINES,5 ROOM,274,TAMPINES ST 22,10 TO 12,105,New Generation,1985,404000",
            fields);
        ASSERT_EQ(fields.size(), 10u);
        ASSERT_EQ(fields[0], "Jan-15");
        ASSERT_EQ(fields[6], "105");
        ASSERT_EQ(fields[9], "404000"); });

  t.run("Quoted field containing comma", []()
        {
        std::vector<std::string> fields;
        parseCSVLine(R"(Jan-15,"TAMPINES, EAST",104000)", fields);
        ASSERT_EQ(fields.size(), 3u);
        ASSERT_EQ(fields[1], "TAMPINES, EAST"); });

  t.run("Escaped double-quote inside quoted field", []()
        {
        std::vector<std::string> fields;
        parseCSVLine(R"(Jan-15,"He said ""hello""",104000)", fields);
        ASSERT_EQ(fields[1], "He said \"hello\""); });

  t.run("Trailing comma produces empty last field", []()
        {
        std::vector<std::string> fields;
        parseCSVLine("Jan-15,TAMPINES,", fields);
        ASSERT_EQ(fields.size(), 3u);
        ASSERT_EQ(fields[2], ""); });

  t.run("Single field (no delimiter) produces one element", []()
        {
        std::vector<std::string> fields;
        parseCSVLine("ONLYFIELD", fields);
        ASSERT_EQ(fields.size(), 1u);
        ASSERT_EQ(fields[0], "ONLYFIELD"); });

  t.run("Fields are trimmed of surrounding whitespace", []()
        {
        std::vector<std::string> fields;
        parseCSVLine(" Jan-15 , TAMPINES , 104000 ", fields);
        ASSERT_EQ(fields[0], "Jan-15");
        ASSERT_EQ(fields[1], "TAMPINES");
        ASSERT_EQ(fields[2], "104000"); });

  t.run("Empty string produces one empty field", []()
        {
        std::vector<std::string> fields;
        parseCSVLine("", fields);
        ASSERT_EQ(fields.size(), 1u);
        ASSERT_EQ(fields[0], ""); });

  t.run("Consecutive commas produce empty intermediate fields", []()
        {
        std::vector<std::string> fields;
        parseCSVLine("a,,b", fields);
        ASSERT_EQ(fields.size(), 3u);
        ASSERT_EQ(fields[1], ""); });
}

// =============================================================================
// Section 4: parseMonthField()
// =============================================================================
static void testParseMonthField(TestRunner &t)
{
  t.section("4. parseMonthField() – valid formats, all months, year mapping, errors");

  t.run("Jan-15 parses to year=2015, month=1", []()
        {
        uint16_t y; uint8_t m;
        parseMonthField("Jan-15", y, m);
        ASSERT_EQ(y, 2015u);
        ASSERT_EQ(m, 1u); });

  t.run("Dec-24 parses to year=2024, month=12", []()
        {
        uint16_t y; uint8_t m;
        parseMonthField("Dec-24", y, m);
        ASSERT_EQ(y, 2024u);
        ASSERT_EQ(m, 12u); });

  t.run("All 12 months map to correct numeric values", []()
        {
        const char* months[] = {
            "Jan","Feb","Mar","Apr","May","Jun",
            "Jul","Aug","Sep","Oct","Nov","Dec"
        };
        for (int i = 0; i < 12; ++i) {
            std::string s = std::string(months[i]) + "-20";
            uint16_t y; uint8_t m;
            parseMonthField(s, y, m);
            ASSERT_EQ(static_cast<int>(m), i + 1);
        } });

  t.run("Case-insensitive month abbreviation (jAN-20)", []()
        {
        uint16_t y; uint8_t m;
        parseMonthField("jAN-20", y, m);
        ASSERT_EQ(m, 1u);
        ASSERT_EQ(y, 2020u); });

  t.run("Year 2025 accepted (data can include 2025)", []()
        {
        uint16_t y; uint8_t m;
        parseMonthField("Jan-25", y, m);
        ASSERT_EQ(y, 2025u); });

  t.run("Year outside [2015,2025] throws", []()
        {
        uint16_t y; uint8_t m;
        ASSERT_THROWS(parseMonthField("Jan-10", y, m)); });

  t.run("Invalid separator throws", []()
        {
        uint16_t y; uint8_t m;
        ASSERT_THROWS(parseMonthField("Jan/15", y, m)); });

  t.run("Unknown month abbreviation throws", []()
        {
        uint16_t y; uint8_t m;
        ASSERT_THROWS(parseMonthField("Xxx-20", y, m)); });

  t.run("Too-short string throws", []()
        {
        uint16_t y; uint8_t m;
        ASSERT_THROWS(parseMonthField("Ja-20", y, m)); });

  t.run("Non-numeric year part throws", []()
        {
        uint16_t y; uint8_t m;
        ASSERT_THROWS(parseMonthField("Jan-AB", y, m)); });
}

// =============================================================================
// Section 5: loadCSV()
// =============================================================================
static void testLoadCSV(TestRunner &t)
{
  t.section("5. loadCSV() – file handling, column detection, row parsing");

  t.run("Loads all valid rows from a 10-column CSV", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10 + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_load_10col.csv");
        ColumnStore db;
        std::size_t n = loadCSV(fname, db);
        ASSERT_EQ(n, 2u);
        ASSERT_EQ(db.size(), 2u);
        std::remove(fname.c_str()); });

  t.run("Handles 11-column CSV (remaining_lease present)", []()
        {
        std::string csv = HEADER_11 + ROW_VALID_11;
        auto fname = writeTmpCSV(csv, "test_load_11col.csv");
        ColumnStore db;
        std::size_t n = loadCSV(fname, db);
        ASSERT_EQ(n, 1u);
        ASSERT_EQ(db.col_resale_price[0], 404000u);   // not confused with lease
        std::remove(fname.c_str()); });

  t.run("Correct field values loaded into columns", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_load_values.csv");
        ColumnStore db;
        loadCSV(fname, db);
        ASSERT_EQ(db.col_month_year[0],            2015u);
        ASSERT_EQ(db.col_month_month[0],           1u);
        ASSERT_EQ(db.col_town[0],                  "TAMPINES");
        ASSERT_EQ(db.col_block[0],                 "274");
        ASSERT_EQ(db.col_floor_area[0],            105u);
        ASSERT_EQ(db.col_lease_commence_date[0],   1985u);
        ASSERT_EQ(db.col_resale_price[0],          404000u);
        std::remove(fname.c_str()); });

  t.run("Nonexistent file throws runtime_error", []()
        {
        ColumnStore db;
        ASSERT_THROWS(loadCSV("does_not_exist_xyz.csv", db)); });

  t.run("Header-only file loads zero records", []()
        {
        auto fname = writeTmpCSV(HEADER_10, "test_load_header_only.csv");
        ColumnStore db;
        std::size_t n = loadCSV(fname, db);
        ASSERT_EQ(n, 0u);
        std::remove(fname.c_str()); });

  t.run("Empty file throws runtime_error", []()
        {
        auto fname = writeTmpCSV("", "test_load_empty.csv");
        ColumnStore db;
        ASSERT_THROWS(loadCSV(fname, db));
        std::remove(fname.c_str()); });

  t.run("Missing required column in header throws", []()
        {
        // Remove 'resale_price' from header
        std::string bad_header =
            "month,town,flat_type,block,street_name,storey_range,"
            "floor_area_sqm,flat_model,lease_commence_date\n";
        auto fname = writeTmpCSV(bad_header + "Jan-15,TAMPINES,5 ROOM,274,ST,10 TO 12,105,New Generation,1985\n",
                                 "test_load_missing_col.csv");
        ColumnStore db;
        ASSERT_THROWS(loadCSV(fname, db));
        std::remove(fname.c_str()); });

  t.run("Row with wrong column count is skipped (not counted)", []()
        {
        // One valid row, one row with too few fields
        std::string csv = HEADER_10 + ROW_VALID_10 +
                          "Jan-15,TAMPINES,5 ROOM\n";  // only 3 fields
        auto fname = writeTmpCSV(csv, "test_load_skip_bad.csv");
        ColumnStore db;
        std::size_t n = loadCSV(fname, db);
        ASSERT_EQ(n, 1u);
        std::remove(fname.c_str()); });

  t.run("Row with empty mandatory field (price) is skipped", []()
        {
        std::string csv = HEADER_10 +
            "Jan-15,TAMPINES,5 ROOM,274,TAMPINES ST 22,10 TO 12,105,New Generation,1985,\n";
        auto fname = writeTmpCSV(csv, "test_load_empty_price.csv");
        ColumnStore db;
        std::size_t n = loadCSV(fname, db);
        ASSERT_EQ(n, 0u);
        std::remove(fname.c_str()); });

  t.run("Blank lines between data rows are ignored", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10 + "\n\n" + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_load_blank_lines.csv");
        ColumnStore db;
        std::size_t n = loadCSV(fname, db);
        ASSERT_EQ(n, 2u);
        std::remove(fname.c_str()); });

  t.run("Year out of range in month field causes row skip", []()
        {
        // Year 2010 is outside [2015,2025]
        std::string csv = HEADER_10 +
            "Jan-10,TAMPINES,5 ROOM,274,TAMPINES ST 22,10 TO 12,105,New Generation,1985,404000\n";
        auto fname = writeTmpCSV(csv, "test_load_bad_year.csv");
        ColumnStore db;
        std::size_t n = loadCSV(fname, db);
        ASSERT_EQ(n, 0u);
        std::remove(fname.c_str()); });

  t.run("Column order robustness: swapped header columns still load correctly", []()
        {
        // Swap price and month in header (and data)
        std::string swapped_header =
            "resale_price,town,flat_type,block,street_name,storey_range,"
            "floor_area_sqm,flat_model,lease_commence_date,month\n";
        std::string swapped_row =
            "404000,TAMPINES,5 ROOM,274,TAMPINES ST 22,10 TO 12,105,New Generation,1985,Jan-15\n";
        auto fname = writeTmpCSV(swapped_header + swapped_row,
                                 "test_load_swap.csv");
        ColumnStore db;
        std::size_t n = loadCSV(fname, db);
        ASSERT_EQ(n, 1u);
        ASSERT_EQ(db.col_resale_price[0], 404000u);
        ASSERT_EQ(db.col_month_year[0], 2015u);
        std::remove(fname.c_str()); });
}

// =============================================================================
// Section 6: buildTownList()
// =============================================================================
static void testBuildTownList(TestRunner &t)
{
  t.section("6. buildTownList() – digit-to-town mapping, deduplication");

  t.run("Digit 0 -> BEDOK", []()
        {
        auto towns = buildTownList("A0000000B");
        ASSERT(std::find(towns.begin(), towns.end(), "BEDOK") != towns.end()); });
  t.run("Digit 5 -> JURONG WEST", []()
        {
        auto towns = buildTownList("A5000000B");
        ASSERT(std::find(towns.begin(), towns.end(), "JURONG WEST") != towns.end()); });
  t.run("Digit 6 -> PASIR RIS", []()
        {
        auto towns = buildTownList("A6000000B");
        ASSERT(std::find(towns.begin(), towns.end(), "PASIR RIS") != towns.end()); });
  t.run("Digit 7 -> TAMPINES", []()
        {
        auto towns = buildTownList("A7000000B");
        ASSERT(std::find(towns.begin(), towns.end(), "TAMPINES") != towns.end()); });
  t.run("Digit 8 -> WOODLANDS", []()
        {
        auto towns = buildTownList("A8000000B");
        ASSERT(std::find(towns.begin(), towns.end(), "WOODLANDS") != towns.end()); });
  t.run("Digit 9 -> YISHUN", []()
        {
        auto towns = buildTownList("A9000000B");
        ASSERT(std::find(towns.begin(), towns.end(), "YISHUN") != towns.end()); });

  t.run("All 10 digits produce 10 distinct towns", []()
        {
        auto towns = buildTownList("A0123456789B");
        ASSERT_EQ(towns.size(), 10u); });

  t.run("Repeated digits are deduplicated (A5656567B -> 3 unique towns)", []()
        {
        // Digits: 5,6,5,6,5,6,7 -> unique: 5,6,7
        auto towns = buildTownList("A5656567B");
        ASSERT_EQ(towns.size(), 3u);
        ASSERT(std::find(towns.begin(), towns.end(), "JURONG WEST") != towns.end());
        ASSERT(std::find(towns.begin(), towns.end(), "PASIR RIS")   != towns.end());
        ASSERT(std::find(towns.begin(), towns.end(), "TAMPINES")    != towns.end()); });

  t.run("Matric with no digits produces empty town list", []()
        {
        auto towns = buildTownList("ABCDE");
        ASSERT(towns.empty()); });

  t.run("Insertion order follows first occurrence in matric string", []()
        {
        // A6626226B -> first digits in order: 6, 2
        auto towns = buildTownList("A6626226B");
        ASSERT_EQ(towns[0], "PASIR RIS");
        ASSERT_EQ(towns[1], "CLEMENTI"); });

  t.run("Non-digit characters in matric are ignored", []()
        {
        // Only digit is 7
        auto towns = buildTownList("AB7CD");
        ASSERT_EQ(towns.size(), 1u);
        ASSERT_EQ(towns[0], "TAMPINES"); });
}

// =============================================================================
// Section 7: deriveQueryParams()
// =============================================================================
static void testDeriveQueryParams(TestRunner &t)
{
  t.section("7. deriveQueryParams() – year/month derivation, edge cases");

  // From spec: last digit -> year via YEAR_MAP; second-last -> start_month
  // YEAR_MAP: 0->2020,1->2021,2->2022,3->2023,4->2024,5->2015,6->2016,7->2017,8->2018,9->2019

  t.run("A5656567B: last=7->2017, second-last=6->June", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A5656567B", y, m);
        ASSERT_EQ(y, 2017u);
        ASSERT_EQ(m, 6u); });

  t.run("A6626226B: last=6->2016, second-last=2->February", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A6626226B", y, m);
        ASSERT_EQ(y, 2016u);
        ASSERT_EQ(m, 2u); });

  t.run("Last digit 0 -> year 2020", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A1230B", y, m);
        ASSERT_EQ(y, 2020u); });

  t.run("Last digit 5 -> year 2015", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A0015B", y, m);
        ASSERT_EQ(y, 2015u); });

  t.run("Last digit 4 -> year 2024", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A0034B", y, m);
        ASSERT_EQ(y, 2024u); });

  t.run("Second-last digit 0 -> start_month = 10 (October)", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A0001B", y, m);  // second-last=0, last=1
        ASSERT_EQ(m, 10u); });

  t.run("Second-last digit 7 -> start_month = 7 (July)", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A0071B", y, m);  // second-last=7, last=1
        ASSERT_EQ(m, 7u); });

  t.run("Trailing non-digit characters are skipped to find digits", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A3245675B", y, m); // last=5->2015, second-last=7->7
        ASSERT_EQ(y, 2015u);
        ASSERT_EQ(m, 7u); });

  t.run("Matric with fewer than 2 digits throws", []()
        {
        uint16_t y; uint8_t m;
        ASSERT_THROWS(deriveQueryParams("ABCDE1F", y, m)); });

  t.run("Matric with no digits throws", []()
        {
        uint16_t y; uint8_t m;
        ASSERT_THROWS(deriveQueryParams("ABCDEF", y, m)); });

  t.run("All 10 last-digit->year mappings are correct", []()
        {
        const uint16_t YEAR_MAP[10] = {
            2020,2021,2022,2023,2024,2015,2016,2017,2018,2019
        };
        for (int d = 0; d <= 9; ++d) {
            // Build matric: second-last=1, last=d
            std::string matric = "A1" + std::to_string(d) + "B";
            uint16_t y; uint8_t m;
            deriveQueryParams(matric, y, m);
            ASSERT_EQ(y, YEAR_MAP[d]);
        } });
}

// =============================================================================
// Section 8: runQuery()
// =============================================================================
static void testRunQuery(TestRunner &t)
{
  t.section("8. runQuery() – predicates, PPSM computation, threshold, no_result");

  // A record that exactly matches: year=2017, month=6, town=TAMPINES,
  // floor_area=100, price=300000 -> ppsm = 3000 (below 4725 threshold)
  auto makeDB = [](uint16_t yr, uint8_t mo, const std::string &town,
                   uint16_t area, uint32_t price)
  {
    return makeSingleRecordDB(yr, mo, town, area, price);
  };

  std::vector<std::string> towns = {"TAMPINES"};

  t.run("Matching record returns no_result=false", [&]()
        {
        auto db = makeDB(2017, 6, "TAMPINES", 100, 300000);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(!r.no_result); });

  t.run("PPSM computed correctly (300000/100 = 3000)", [&]()
        {
        auto db = makeDB(2017, 6, "TAMPINES", 100, 300000);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT_NEAR(r.price_per_sqm, 3000.0, 0.001); });

  t.run("Wrong year -> no_result=true", [&]()
        {
        auto db = makeDB(2018, 6, "TAMPINES", 100, 300000);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Month before window -> no_result=true", [&]()
        {
        auto db = makeDB(2017, 5, "TAMPINES", 100, 300000); // month=5, window=[6,6]
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Month after window -> no_result=true", [&]()
        {
        auto db = makeDB(2017, 9, "TAMPINES", 100, 300000); // window=[6,8] for x=3
        QueryResult r;
        runQuery(db, 3, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Month at end of window (inclusive) -> matches", [&]()
        {
        auto db = makeDB(2017, 8, "TAMPINES", 100, 300000); // window=[6,8] for x=3
        QueryResult r;
        runQuery(db, 3, 80, 2017, 6, towns, r);
        ASSERT(!r.no_result); });

  t.run("Wrong town -> no_result=true", [&]()
        {
        auto db = makeDB(2017, 6, "BEDOK", 100, 300000);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Floor area exactly at y threshold -> matches", [&]()
        {
        auto db = makeDB(2017, 6, "TAMPINES", 80, 300000); // area=80, y=80
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(!r.no_result); });

  t.run("Floor area one below y threshold -> no_result=true", [&]()
        {
        auto db = makeDB(2017, 6, "TAMPINES", 79, 300000); // area=79, y=80
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("PPSM exactly at 4725 threshold -> valid (not excluded)", [&]()
        {
        // price=472500, area=100 -> ppsm=4725.0
        auto db = makeDB(2017, 6, "TAMPINES", 100, 472500);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(!r.no_result);
        ASSERT_NEAR(r.price_per_sqm, 4725.0, 0.001); });

  t.run("PPSM above 4725 -> no_result=true", [&]()
        {
        // price=472600, area=100 -> ppsm=4726.0
        auto db = makeDB(2017, 6, "TAMPINES", 100, 472600);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Record with minimum PPSM is selected among multiple", []()
        {
        ColumnStore db;
        // Record A: ppsm = 300000/100 = 3000
        db.col_month_year.push_back(2017); db.col_month_month.push_back(6);
        db.col_town.push_back("TAMPINES");
        db.col_block.push_back("10");
        db.col_street_name.push_back("ST A");
        db.col_flat_type.push_back("4 ROOM");
        db.col_flat_model.push_back("Improved");
        db.col_storey_range.push_back("01 TO 03");
        db.col_floor_area.push_back(100);
        db.col_lease_commence_date.push_back(1990);
        db.col_resale_price.push_back(300000);

        // Record B: ppsm = 260000/100 = 2600  (cheaper, should be selected)
        db.col_month_year.push_back(2017); db.col_month_month.push_back(6);
        db.col_town.push_back("TAMPINES");
        db.col_block.push_back("20");
        db.col_street_name.push_back("ST B");
        db.col_flat_type.push_back("4 ROOM");
        db.col_flat_model.push_back("New Generation");
        db.col_storey_range.push_back("04 TO 06");
        db.col_floor_area.push_back(100);
        db.col_lease_commence_date.push_back(1985);
        db.col_resale_price.push_back(260000);

        QueryResult r;
        std::vector<std::string> towns = {"TAMPINES"};
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(!r.no_result);
        ASSERT_NEAR(r.price_per_sqm, 2600.0, 0.001);
        ASSERT_EQ(r.block, "20"); });

  t.run("x and y are stored in result", [&]()
        {
        auto db = makeDB(2017, 6, "TAMPINES", 100, 300000);
        QueryResult r;
        runQuery(db, 3, 95, 2017, 6, towns, r);
        ASSERT_EQ(r.x, 3);
        ASSERT_EQ(r.y, 95); });

  t.run("Multiple towns in list – record in second town matches", []()
        {
        auto db = makeSingleRecordDB(2017, 6, "PASIR RIS", 100, 300000);
        std::vector<std::string> towns2 = {"TAMPINES", "PASIR RIS"};
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns2, r);
        ASSERT(!r.no_result);
        ASSERT_EQ(r.town, "PASIR RIS"); });

  t.run("Window capped at month 12 (start=10, x=5 -> end=12 not 14)", []()
        {
        // Month 12 should be included; month 13 doesn't exist
        auto db = makeSingleRecordDB(2017, 12, "TAMPINES", 100, 300000);
        std::vector<std::string> localTowns = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 5, 80, 2017, 10, localTowns, r);  // window=[10,12]
        ASSERT(!r.no_result); });

  t.run("Empty database -> no_result=true", []()
        {
        ColumnStore db;
        std::vector<std::string> towns = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("All metadata fields populated correctly from matching record", []()
        {
        ColumnStore db = makeSingleRecordDB(
            2017, 6, "TAMPINES", 100, 300000, "99", "Premium", 1992);
        std::vector<std::string> towns = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT_EQ(r.year,               2017u);
        ASSERT_EQ(r.month,              6u);
        ASSERT_EQ(r.town,               "TAMPINES");
        ASSERT_EQ(r.block,              "99");
        ASSERT_EQ(r.floor_area,         100u);
        ASSERT_EQ(r.flat_model,         "Premium");
        ASSERT_EQ(r.lease_commence_date, 1992u); });
}

// =============================================================================
// Section 9: writeResults()
// =============================================================================
static void testWriteResults(TestRunner &t)
{
  t.section("9. writeResults() – CSV format, zero-pad month, rounding, no_result");

  // Helper: read file into string
  auto readFile = [](const std::string &fname) -> std::string
  {
    std::ifstream f(fname);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
  };

  t.run("Output file is created with correct name", []()
        {
        std::vector<QueryResult> results;
        QueryResult r;
        r.no_result = true;
        results.push_back(r);
        writeResults(results, "TESTMATRIC");
        std::ifstream f("ScanResult_TESTMATRIC.csv");
        ASSERT(f.is_open());
        f.close();
        std::remove("ScanResult_TESTMATRIC.csv"); });

  t.run("Header row matches specification exactly", [&readFile]()
        {
        std::vector<QueryResult> results;
        writeResults(results, "TEST_HDR");
        std::string content = readFile("ScanResult_TEST_HDR.csv");
        ASSERT(content.find("(x, y),Year,Month,Town,Block,Floor_Area,"
                            "Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter")
               != std::string::npos);
        std::remove("ScanResult_TEST_HDR.csv"); });

  t.run("Valid result is written in correct CSV format", [&readFile]()
        {
        QueryResult r;
        r.x = 1; r.y = 80;
        r.no_result = false;
        r.year = 2016; r.month = 2;
        r.town = "PASIR RIS"; r.block = "149";
        r.floor_area = 126;
        r.flat_model = "Improved";
        r.lease_commence_date = 1995;
        r.price_per_sqm = 3413.0;
        std::vector<QueryResult> results = {r};
        writeResults(results, "TEST_ROW");
        std::string content = readFile("ScanResult_TEST_ROW.csv");
        ASSERT(content.find("(1, 80),2016,02,PASIR RIS,149,126,Improved,1995,3413")
               != std::string::npos);
        std::remove("ScanResult_TEST_ROW.csv"); });

  t.run("Single-digit month is zero-padded to 2 digits", [&readFile]()
        {
        QueryResult r;
        r.x = 1; r.y = 80; r.no_result = false;
        r.year = 2017; r.month = 6;   // <-- single digit
        r.town = "TAMPINES"; r.block = "1";
        r.floor_area = 100; r.flat_model = "Improved";
        r.lease_commence_date = 1990; r.price_per_sqm = 3000.0;
        std::vector<QueryResult> results = {r};
        writeResults(results, "TEST_PAD");
        std::string content = readFile("ScanResult_TEST_PAD.csv");
        ASSERT(content.find(",06,") != std::string::npos);
        std::remove("ScanResult_TEST_PAD.csv"); });

  t.run("PPSM is rounded to nearest integer (3413.4 -> 3413)", [&readFile]()
        {
        QueryResult r;
        r.x = 1; r.y = 80; r.no_result = false;
        r.year = 2017; r.month = 6;
        r.town = "TAMPINES"; r.block = "1";
        r.floor_area = 100; r.flat_model = "Improved";
        r.lease_commence_date = 1990; r.price_per_sqm = 3413.4;
        std::vector<QueryResult> results = {r};
        writeResults(results, "TEST_ROUND_DOWN");
        std::string content = readFile("ScanResult_TEST_ROUND_DOWN.csv");
        ASSERT(content.find(",3413\n") != std::string::npos);
        std::remove("ScanResult_TEST_ROUND_DOWN.csv"); });

  t.run("PPSM rounds up at 0.5 (3413.5 -> 3414)", [&readFile]()
        {
        QueryResult r;
        r.x = 1; r.y = 80; r.no_result = false;
        r.year = 2017; r.month = 6;
        r.town = "TAMPINES"; r.block = "1";
        r.floor_area = 100; r.flat_model = "Improved";
        r.lease_commence_date = 1990; r.price_per_sqm = 3413.5;
        std::vector<QueryResult> results = {r};
        writeResults(results, "TEST_ROUND_UP");
        std::string content = readFile("ScanResult_TEST_ROUND_UP.csv");
        ASSERT(content.find(",3414\n") != std::string::npos);
        std::remove("ScanResult_TEST_ROUND_UP.csv"); });

  t.run("Results with no_result=true are omitted from output", [&readFile]()
        {
        QueryResult r;
        r.x = 2; r.y = 100; r.no_result = true;  // should be skipped
        std::vector<QueryResult> results = {r};
        writeResults(results, "TEST_NORESULT");
        std::string content = readFile("ScanResult_TEST_NORESULT.csv");
        // Only header row should be present (no data rows)
        size_t newlines = std::count(content.begin(), content.end(), '\n');
        ASSERT_EQ(newlines, 1u);   // only the header line
        std::remove("ScanResult_TEST_NORESULT.csv"); });

  t.run("Multiple results written in order", [&readFile]()
        {
        std::vector<QueryResult> results;
        for (int x = 1; x <= 3; ++x) {
            QueryResult r;
            r.x = x; r.y = 80; r.no_result = false;
            r.year = 2017; r.month = 6;
            r.town = "TAMPINES"; r.block = std::to_string(x);
            r.floor_area = 100; r.flat_model = "Improved";
            r.lease_commence_date = 1990; r.price_per_sqm = 3000.0;
            results.push_back(r);
        }
        writeResults(results, "TEST_MULTI");
        std::string content = readFile("ScanResult_TEST_MULTI.csv");
        size_t newlines = std::count(content.begin(), content.end(), '\n');
        ASSERT_EQ(newlines, 4u);  // 1 header + 3 data rows
        std::remove("ScanResult_TEST_MULTI.csv"); });
}

// =============================================================================
// Section 10: Integration – end-to-end pipeline
// =============================================================================
static void testIntegration(TestRunner &t)
{
  t.section("10. Integration – end-to-end pipeline on known mini-dataset");

  // Build a small CSV covering the spec example:
  //   Matric A6626226B -> towns: PASIR RIS, CLEMENTI; year=2016; start_month=2
  //
  // We craft records so that for (x=1, y=80):
  //   - Only record P1 qualifies (PASIR RIS, Feb 2016, area=126, price=430038)
  //   - ppsm = 430038/126 = 3413.0 (matches spec example output)

  const std::string matric = "A6626226B";

  // price = 3413 * 126 = 430038
  std::string csv = HEADER_10 +
                    // Qualifying record
                    "Jan-16,PASIR RIS,4 ROOM,149,PASIR RIS CTRL,07 TO 09,126,Improved,1995,430038\n"
                    // Wrong town (should be excluded)
                    "Jan-16,TAMPINES,4 ROOM,10,TAMPINES ST 1,01 TO 03,130,New Generation,1990,350000\n"
                    // Wrong year (should be excluded)
                    "Jan-17,PASIR RIS,4 ROOM,200,PASIR RIS DR,01 TO 03,100,Standard,1988,300000\n"
                    // Area too small (should be excluded for y=150)
                    "Jan-16,PASIR RIS,3 ROOM,50,PASIR RIS LNK,01 TO 03,60,Improved,1980,200000\n";

  auto fname = writeTmpCSV(csv, "test_integration.csv");

  // Load
  ColumnStore db;
  std::size_t n = loadCSV(fname, db);
  ASSERT_EQ(n, 4u);

  // Derive params
  uint16_t target_year;
  uint8_t start_month;
  deriveQueryParams(matric, target_year, start_month);
  ASSERT_EQ(target_year, 2016u);
  ASSERT_EQ(start_month, 2u);

  // Build town list
  auto towns = buildTownList(matric);
  // A6626226B -> digits 6,2 -> PASIR RIS, CLEMENTI
  ASSERT_EQ(towns.size(), 2u);

  t.run("Integration: load count correct", [&]()
        { ASSERT_EQ(n, 4u); });

  t.run("Integration: derived year=2016, month=2 for A6626226B", [&]()
        {
        ASSERT_EQ(target_year, 2016u);
        ASSERT_EQ(start_month, 2u); });

  t.run("Integration: town list has PASIR RIS and CLEMENTI", [&]()
        {
        ASSERT(std::find(towns.begin(), towns.end(), "PASIR RIS") != towns.end());
        ASSERT(std::find(towns.begin(), towns.end(), "CLEMENTI")  != towns.end()); });

  t.run("Integration: (x=1,y=80) finds qualifying record in PASIR RIS Jan 2016", [&]()
        {
        // start_month=2, x=1 -> window=[2,2] (February)
        // Our only PASIR RIS record is in January (month=1) -> outside window
        // So this should actually be no_result for a strict window.
        // Let's verify: month=1 < start_month=2 -> excluded
        QueryResult r;
        runQuery(db, 1, 80, target_year, start_month, towns, r);
        // Our fixture record is month 1 (Jan), start_month=2, so it falls outside
        ASSERT(r.no_result); });

  t.run("Integration: (x=2,y=80) with window=[2,3] still misses Jan record", [&]()
        {
        QueryResult r;
        runQuery(db, 2, 80, target_year, start_month, towns, r);
        ASSERT(r.no_result); });

  // Add a February record to test a passing case
  t.run("Integration: February record within window qualifies and selects min PPSM", [&]()
        {
        ColumnStore db2;
        // Feb-16, PASIR RIS, area=126, price=430038 -> ppsm=3413.0
        db2.col_month_year.push_back(2016);  db2.col_month_month.push_back(2);
        db2.col_town.push_back("PASIR RIS"); db2.col_block.push_back("149");
        db2.col_street_name.push_back("PASIR RIS CTRL");
        db2.col_flat_type.push_back("4 ROOM");
        db2.col_flat_model.push_back("Improved");
        db2.col_storey_range.push_back("07 TO 09");
        db2.col_floor_area.push_back(126);
        db2.col_lease_commence_date.push_back(1995);
        db2.col_resale_price.push_back(430038);

        QueryResult r;
        runQuery(db2, 1, 80, 2016, 2, towns, r);
        ASSERT(!r.no_result);
        ASSERT_EQ(r.town,       "PASIR RIS");
        ASSERT_EQ(r.block,      "149");
        ASSERT_EQ(r.floor_area, 126u);
        ASSERT_EQ(r.lease_commence_date, 1995u);
        ASSERT_NEAR(r.price_per_sqm, 3413.0, 0.1); });

  t.run("Integration: y=150 excludes area=126 record", [&]()
        {
        ColumnStore db2;
        db2.col_month_year.push_back(2016);  db2.col_month_month.push_back(2);
        db2.col_town.push_back("PASIR RIS"); db2.col_block.push_back("149");
        db2.col_street_name.push_back("PASIR RIS CTRL");
        db2.col_flat_type.push_back("4 ROOM");
        db2.col_flat_model.push_back("Improved");
        db2.col_storey_range.push_back("07 TO 09");
        db2.col_floor_area.push_back(126);
        db2.col_lease_commence_date.push_back(1995);
        db2.col_resale_price.push_back(430038);

        QueryResult r;
        runQuery(db2, 1, 150, 2016, 2, towns, r);  // y=150 > area=126
        ASSERT(r.no_result); });

  t.run("Integration: full output written with correct spec header", [&]()
        {
        // Write one known-good result and verify the output file is spec-compliant
        QueryResult r;
        r.x = 1; r.y = 80; r.no_result = false;
        r.year = 2016; r.month = 2;
        r.town = "PASIR RIS"; r.block = "149";
        r.floor_area = 126; r.flat_model = "Improved";
        r.lease_commence_date = 1995; r.price_per_sqm = 3413.0;
        std::vector<QueryResult> results = {r};
        writeResults(results, matric);

        std::ifstream f("ScanResult_" + matric + ".csv");
        ASSERT(f.is_open());
        std::string line1;
        std::getline(f, line1);
        ASSERT_EQ(line1,
            "(x, y),Year,Month,Town,Block,Floor_Area,"
            "Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter");
        std::string line2;
        std::getline(f, line2);
        ASSERT_EQ(line2, "(1, 80),2016,02,PASIR RIS,149,126,Improved,1995,3413");
        f.close();
        std::remove(("ScanResult_" + matric + ".csv").c_str()); });

  std::remove(fname.c_str());
}

// =============================================================================
// Section 11: Boundary / Spec Constraints
// =============================================================================
static void testBoundaryConditions(TestRunner &t)
{
  t.section("11. Boundary conditions from project spec (x, y ranges)");

  std::vector<std::string> towns = {"TAMPINES"};

  t.run("x=1 (minimum): window is exactly one month", []()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 100, 300000);
        std::vector<std::string> t2 = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, t2, r);
        ASSERT(!r.no_result); });

  t.run("x=8 (maximum): window spans 8 months", []()
        {
        // start_month=2, x=8 -> end=9; record in Sep should match
        auto db = makeSingleRecordDB(2017, 9, "TAMPINES", 100, 300000);
        std::vector<std::string> t2 = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 8, 80, 2017, 2, t2, r);
        ASSERT(!r.no_result); });

  t.run("y=80 (minimum area): record with area=80 qualifies", []()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 80, 300000);
        std::vector<std::string> t2 = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, t2, r);
        ASSERT(!r.no_result); });

  t.run("y=150 (maximum area): record with area=150 qualifies", []()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 150, 300000);
        std::vector<std::string> t2 = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 1, 150, 2017, 6, t2, r);
        ASSERT(!r.no_result); });

  t.run("y=150: record with area=149 does not qualify", []()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 149, 300000);
        std::vector<std::string> t2 = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 1, 150, 2017, 6, t2, r);
        ASSERT(r.no_result); });

  t.run("Target year cannot be 2025 per spec (YEAR_MAP has no 2025)", []()
        {
        // Verify that digit->year mapping never produces 2025
        const uint16_t YEAR_MAP[10] = {
            2020,2021,2022,2023,2024,2015,2016,2017,2018,2019
        };
        for (int d = 0; d <= 9; ++d) {
            ASSERT(YEAR_MAP[d] != 2025u);
        } });

  t.run("Start month=10 (digit 0 in matric) -> October", []()
        {
        uint16_t y; uint8_t m;
        deriveQueryParams("A1230B", y, m);   // second-last=3, last=0 -> month=3
        // Wait: last=0->2020, second-last=3->3
        ASSERT_EQ(m, 3u);
        // Test the 0->October mapping specifically
        deriveQueryParams("A0001B", y, m);  // second-last=0->10, last=1->2021
        ASSERT_EQ(m, 10u); });
}
// =============================================================================
// Section 12: DictionaryEncoder – unit tests
// =============================================================================
static void testDictionaryEncoder(TestRunner &t)
{
  t.section("12. DictionaryEncoder – unit tests");

  t.run("First encode assigns ID 0", []()
        {
        DictionaryEncoder enc;
        ASSERT_EQ(enc.encode("TAMPINES"), 0u); });

  t.run("Same string returns same ID", []()
        {
        DictionaryEncoder enc;
        uint16_t id1 = enc.encode("TAMPINES");
        uint16_t id2 = enc.encode("TAMPINES");
        ASSERT_EQ(id1, id2); });

  t.run("Different strings get different IDs", []()
        {
        DictionaryEncoder enc;
        uint16_t id1 = enc.encode("TAMPINES");
        uint16_t id2 = enc.encode("BEDOK");
        ASSERT(id1 != id2); });

  t.run("IDs are assigned sequentially (0, 1, 2...)", []()
        {
        DictionaryEncoder enc;
        ASSERT_EQ(enc.encode("TAMPINES"),    0u);
        ASSERT_EQ(enc.encode("BEDOK"),       1u);
        ASSERT_EQ(enc.encode("JURONG WEST"), 2u); });

  t.run("decode reverses encode", []()
        {
        DictionaryEncoder enc;
        uint16_t id = enc.encode("PASIR RIS");
        ASSERT_EQ(enc.decode(id), "PASIR RIS"); });

  t.run("lookup finds existing string", []()
        {
        DictionaryEncoder enc;
        enc.encode("CLEMENTI");
        uint16_t id;
        bool found = enc.lookup("CLEMENTI", id);
        ASSERT(found);
        ASSERT_EQ(id, 0u); });

  t.run("lookup returns false for missing string", []()
        {
        DictionaryEncoder enc;
        enc.encode("CLEMENTI");
        uint16_t id;
        bool found = enc.lookup("YISHUN", id);
        ASSERT(!found); });

  t.run("size tracks unique entries", []()
        {
        DictionaryEncoder enc;
        enc.encode("A"); enc.encode("B"); enc.encode("A");
        ASSERT_EQ(enc.size(), 2u); });

  t.run("clear resets encoder completely", []()
        {
        DictionaryEncoder enc;
        enc.encode("TAMPINES");
        enc.clear();
        ASSERT_EQ(enc.size(), 0u);
        // after clear, re-encoding should assign ID 0 again
        ASSERT_EQ(enc.encode("BEDOK"), 0u); });
}

// =============================================================================
// Section 13: Dictionary Encoding – loadCSV populates encoded columns
// =============================================================================
static void testDictEncodingLoad(TestRunner &t)
{
  t.section("13. Dictionary Encoding (A1) – loadCSV populates encoded columns");

  t.run("Encoded columns populated when flag is ON", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_dict_load_on.csv");
        ColumnStore db;
        db.use_dict_encoding = true;
        loadCSV(fname, db);
        ASSERT_EQ(db.col_town_encoded.size(), 1u);
        ASSERT_EQ(db.col_flat_type_encoded.size(), 1u);
        ASSERT_EQ(db.col_flat_model_encoded.size(), 1u);
        ASSERT_EQ(db.col_street_name_encoded.size(), 1u);
        std::remove(fname.c_str()); });

  t.run("Encoded columns EMPTY when flag is OFF", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_dict_load_off.csv");
        ColumnStore db;
        db.use_dict_encoding = false;
        loadCSV(fname, db);
        ASSERT(db.col_town_encoded.empty());
        ASSERT(db.col_flat_type_encoded.empty());
        std::remove(fname.c_str()); });

  t.run("String columns still populated when encoding is ON", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_dict_load_strings.csv");
        ColumnStore db;
        db.use_dict_encoding = true;
        loadCSV(fname, db);
        ASSERT_EQ(db.col_town[0], "TAMPINES");
        ASSERT_EQ(db.col_flat_model[0], "New Generation");
        std::remove(fname.c_str()); });

  t.run("Dictionary correctly maps town string to encoded ID", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_dict_load_map.csv");
        ColumnStore db;
        db.use_dict_encoding = true;
        loadCSV(fname, db);
        // decode the encoded ID back and check it matches the string column
        uint16_t enc_id = db.col_town_encoded[0];
        ASSERT_EQ(db.dict_town.decode(enc_id), "TAMPINES");
        std::remove(fname.c_str()); });

  t.run("Multiple rows with same town get same encoded ID", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10 + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_dict_load_dedup.csv");
        ColumnStore db;
        db.use_dict_encoding = true;
        loadCSV(fname, db);
        ASSERT_EQ(db.col_town_encoded[0], db.col_town_encoded[1]);
        ASSERT_EQ(db.dict_town.size(), 1u);  // only 1 unique town
        std::remove(fname.c_str()); });

  t.run("Multiple different towns get different encoded IDs", []()
        {
        std::string csv = HEADER_10 +
            "Jan-15,TAMPINES,5 ROOM,274,TAMPINES ST 22,10 TO 12,105,New Generation,1985,404000\n"
            "Jan-15,BEDOK,5 ROOM,100,BEDOK NORTH ST 3,01 TO 03,110,Improved,1990,350000\n";
        auto fname = writeTmpCSV(csv, "test_dict_load_multi.csv");
        ColumnStore db;
        db.use_dict_encoding = true;
        loadCSV(fname, db);
        ASSERT(db.col_town_encoded[0] != db.col_town_encoded[1]);
        ASSERT_EQ(db.dict_town.size(), 2u);
        std::remove(fname.c_str()); });

  t.run("Encoded column size matches record count", []()
        {
        std::string csv = HEADER_10 + ROW_VALID_10 + ROW_VALID_10 + ROW_VALID_10;
        auto fname = writeTmpCSV(csv, "test_dict_load_count.csv");
        ColumnStore db;
        db.use_dict_encoding = true;
        loadCSV(fname, db);
        ASSERT_EQ(db.col_town_encoded.size(), db.size());
        ASSERT_EQ(db.col_flat_model_encoded.size(), db.size());
        std::remove(fname.c_str()); });

  t.run("11-column CSV with encoding still works correctly", []()
        {
        std::string csv = HEADER_11 + ROW_VALID_11;
        auto fname = writeTmpCSV(csv, "test_dict_load_11col.csv");
        ColumnStore db;
        db.use_dict_encoding = true;
        loadCSV(fname, db);
        ASSERT_EQ(db.size(), 1u);
        ASSERT_EQ(db.col_town_encoded.size(), 1u);
        ASSERT_EQ(db.col_resale_price[0], 404000u);
        std::remove(fname.c_str()); });
}

// =============================================================================
// Section 14: Dictionary Encoding – runQuery correctness
// =============================================================================
static void testDictEncodingQuery(TestRunner &t)
{
  t.section("14. Dictionary Encoding (A1) – runQuery uses encoded path");

  std::vector<std::string> towns = {"TAMPINES"};

  t.run("Encoded: matching record returns no_result=false", [&]()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 100, 300000,
                                     "1", "Improved", 1990, true);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(!r.no_result); });

  t.run("Encoded: PPSM computed correctly (300000/100 = 3000)", [&]()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 100, 300000,
                                     "1", "Improved", 1990, true);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT_NEAR(r.price_per_sqm, 3000.0, 0.001); });

  t.run("Encoded: wrong year -> no_result=true", [&]()
        {
        auto db = makeSingleRecordDB(2018, 6, "TAMPINES", 100, 300000,
                                     "1", "Improved", 1990, true);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Encoded: wrong town -> no_result=true", [&]()
        {
        auto db = makeSingleRecordDB(2017, 6, "BEDOK", 100, 300000,
                                     "1", "Improved", 1990, true);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Encoded: area below threshold -> no_result=true", [&]()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 79, 300000,
                                     "1", "Improved", 1990, true);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Encoded: PPSM above 4725 -> no_result=true", [&]()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 100, 472600,
                                     "1", "Improved", 1990, true);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Encoded: month outside window -> no_result=true", [&]()
        {
        auto db = makeSingleRecordDB(2017, 5, "TAMPINES", 100, 300000,
                                     "1", "Improved", 1990, true);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });

  t.run("Encoded: multiple towns in filter list", []()
        {
        auto db = makeSingleRecordDB(2017, 6, "PASIR RIS", 100, 300000,
                                     "1", "Improved", 1990, true);
        std::vector<std::string> towns2 = {"TAMPINES", "PASIR RIS"};
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns2, r);
        ASSERT(!r.no_result);
        ASSERT_EQ(r.town, "PASIR RIS"); });

  t.run("Encoded: town in filter but not in data -> no_result", []()
        {
        // DB has TAMPINES, but we only query for WOODLANDS
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 100, 300000,
                                     "1", "Improved", 1990, true);
        std::vector<std::string> towns2 = {"WOODLANDS"};
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns2, r);
        ASSERT(r.no_result); });

  t.run("Encoded: selects minimum PPSM among multiple records", []()
        {
        ColumnStore db;
        db.use_dict_encoding = true;

        // Record A: ppsm = 300000/100 = 3000
        db.col_month_year.push_back(2017); db.col_month_month.push_back(6);
        db.col_town.push_back("TAMPINES"); db.col_block.push_back("10");
        db.col_street_name.push_back("ST A"); db.col_flat_type.push_back("4 ROOM");
        db.col_flat_model.push_back("Improved"); db.col_storey_range.push_back("01 TO 03");
        db.col_floor_area.push_back(100); db.col_lease_commence_date.push_back(1990);
        db.col_resale_price.push_back(300000);
        db.col_town_encoded.push_back(db.dict_town.encode("TAMPINES"));
        db.col_flat_type_encoded.push_back(db.dict_flat_type.encode("4 ROOM"));
        db.col_flat_model_encoded.push_back(db.dict_flat_model.encode("Improved"));
        db.col_street_name_encoded.push_back(db.dict_street_name.encode("ST A"));

        // Record B: ppsm = 260000/100 = 2600 (cheaper, should be selected)
        db.col_month_year.push_back(2017); db.col_month_month.push_back(6);
        db.col_town.push_back("TAMPINES"); db.col_block.push_back("20");
        db.col_street_name.push_back("ST B"); db.col_flat_type.push_back("4 ROOM");
        db.col_flat_model.push_back("New Generation"); db.col_storey_range.push_back("04 TO 06");
        db.col_floor_area.push_back(100); db.col_lease_commence_date.push_back(1985);
        db.col_resale_price.push_back(260000);
        db.col_town_encoded.push_back(db.dict_town.encode("TAMPINES"));
        db.col_flat_type_encoded.push_back(db.dict_flat_type.encode("4 ROOM"));
        db.col_flat_model_encoded.push_back(db.dict_flat_model.encode("New Generation"));
        db.col_street_name_encoded.push_back(db.dict_street_name.encode("ST B"));

        QueryResult r;
        std::vector<std::string> towns = {"TAMPINES"};
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(!r.no_result);
        ASSERT_NEAR(r.price_per_sqm, 2600.0, 0.001);
        ASSERT_EQ(r.block, "20"); });

  t.run("Encoded: output fields are original strings (not IDs)", [&]()
        {
        auto db = makeSingleRecordDB(2017, 6, "TAMPINES", 100, 300000,
                                     "99", "Premium", 1992, true);
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT_EQ(r.town, "TAMPINES");
        ASSERT_EQ(r.flat_model, "Premium");
        ASSERT_EQ(r.block, "99"); });

  t.run("Encoded: empty database -> no_result=true", []()
        {
        ColumnStore db;
        db.use_dict_encoding = true;
        std::vector<std::string> towns = {"TAMPINES"};
        QueryResult r;
        runQuery(db, 1, 80, 2017, 6, towns, r);
        ASSERT(r.no_result); });
}

// =============================================================================
// Section 15: Dictionary Encoding – baseline vs encoded parity
// =============================================================================
static void testDictEncodingParity(TestRunner &t)
{
  t.section("15. Dictionary Encoding (A1) – baseline vs encoded result parity");

  // Build identical CSV with multiple towns, months, areas
  std::string csv = HEADER_10 +
                    "Jun-17,TAMPINES,4 ROOM,100,TAMPINES ST 1,07 TO 09,110,Improved,1990,350000\n"
                    "Jun-17,BEDOK,3 ROOM,200,BEDOK NORTH ST 3,01 TO 03,85,Standard,1985,240000\n"
                    "Jul-17,TAMPINES,5 ROOM,101,TAMPINES ST 2,10 TO 12,130,New Generation,1988,400000\n"
                    "Aug-17,PASIR RIS,4 ROOM,50,PASIR RIS DR 6,04 TO 06,100,Improved,1995,280000\n"
                    "Jun-17,JURONG WEST,3 ROOM,300,JURONG WEST ST 42,01 TO 03,90,Standard,1980,200000\n"
                    "Sep-17,TAMPINES,4 ROOM,102,TAMPINES ST 3,07 TO 09,95,Improved,1992,310000\n";

  t.run("Parity: identical results for all (x,y) pairs across baseline and encoded", [&]()
        {
        auto fname = writeTmpCSV(csv, "test_parity.csv");

        // Load baseline
        ColumnStore db_base;
        db_base.use_dict_encoding = false;
        loadCSV(fname, db_base);

        // Load with dict encoding
        ColumnStore db_dict;
        db_dict.use_dict_encoding = true;
        loadCSV(fname, db_dict);

        std::vector<std::string> towns = {"TAMPINES", "PASIR RIS"};

        // test a range of (x,y) pairs
        for (int x = 1; x <= 4; ++x) {
            for (int y = 80; y <= 150; y += 10) {
                QueryResult r_base, r_dict;
                runQuery(db_base, x, y, 2017, 6, towns, r_base);
                runQuery(db_dict, x, y, 2017, 6, towns, r_dict);

                if (r_base.no_result != r_dict.no_result) {
                    throw std::runtime_error(
                        "Parity mismatch at x=" + std::to_string(x) +
                        " y=" + std::to_string(y) +
                        ": base.no_result=" + std::to_string(r_base.no_result) +
                        " dict.no_result=" + std::to_string(r_dict.no_result));
                }
                if (!r_base.no_result) {
                    if (std::fabs(r_base.price_per_sqm - r_dict.price_per_sqm) > 0.001) {
                        throw std::runtime_error(
                            "PPSM mismatch at x=" + std::to_string(x) +
                            " y=" + std::to_string(y));
                    }
                    if (r_base.town != r_dict.town || r_base.block != r_dict.block) {
                        throw std::runtime_error(
                            "Record mismatch at x=" + std::to_string(x) +
                            " y=" + std::to_string(y));
                    }
                }
            }
        }
        std::remove(fname.c_str()); });

  t.run("Parity: loadCSV via file produces correct encoded columns", [&]()
        {
        auto fname = writeTmpCSV(csv, "test_parity_load.csv");
        ColumnStore db;
        db.use_dict_encoding = true;
        std::size_t n = loadCSV(fname, db);

        // verify every encoded ID decodes back to the original string
        for (std::size_t i = 0; i < n; ++i) {
            ASSERT_EQ(db.dict_town.decode(db.col_town_encoded[i]),
                      db.col_town[i]);
            ASSERT_EQ(db.dict_flat_model.decode(db.col_flat_model_encoded[i]),
                      db.col_flat_model[i]);
        }
        std::remove(fname.c_str()); });
}

// =============================================================================
// main
// =============================================================================
int main()
{
  std::cout << "========================================\n";
  std::cout << "  HDB Column Store – Baseline Test Suite\n";
  std::cout << "========================================\n";

  TestRunner t;

  testColumnStore(t);
  testTrim(t);
  testParseCSVLine(t);
  testParseMonthField(t);
  testLoadCSV(t);
  testBuildTownList(t);
  testDeriveQueryParams(t);
  testRunQuery(t);
  testWriteResults(t);
  testIntegration(t);
  testBoundaryConditions(t);
  testDictionaryEncoder(t);
  testDictEncodingLoad(t);
  testDictEncodingQuery(t);
  testDictEncodingParity(t);

  t.summary();
  return 0;
}