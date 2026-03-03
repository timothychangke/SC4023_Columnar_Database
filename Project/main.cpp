/*
 * main entry point for the SC4023 big data project.
 * this file is intentionally kept thin. it just orchestrates the main flow:
 * 1. parse the matriculation number from cmd args
 * 2. figure out the query params from it
 * 3. ingest ResalePricesSingapore.csv into our column store
 * 4. run all the (x, y) queries and write to csv
 *
 * Usage:
 * ./column_store <MatriculationNumber>
 * eg ./column_store U1234567A
 */

#include <iostream>
#include <stdexcept>
#include <vector>

#include "column_store.h"
#include "csv_parser.h"
#include "output_writer.h"
#include "query_engine.h"

int main(int argc, char* argv[]) {

    // phase 0: check command line args
    // if they never pass in matric number, just scold them and exit
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <MatriculationNumber>\n";
        std::cerr << "Example: " << argv[0] << " A5656567B\n";
        return 1;
    }
    const std::string matric_number = argv[1];
    std::cout << "Matriculation number : " << matric_number << "\n";

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

    // print out the derived params just for our own debugging 
    // so we know immediately if the parser screwed up
    std::cout << "Target year  : " << target_year << "\n";
    std::cout << "Start month  : " << static_cast<int>(start_month) << "\n";
    std::cout << "Target towns : ";
    for (std::size_t i = 0; i < towns.size(); ++i) {
        std::cout << towns[i];
        if (i + 1 < towns.size()) std::cout << ", ";
    }
    std::cout << "\n";

    // phase 2: ingest dataset
    // 
    // dump everything into our in-memory column store
    ColumnStore db;

    try {
        // hardcoded path here. make sure you run the executable from 
        // the correct working directory or it cannot find the file.
        loadCSV("../data/ResalePricesSingapore.csv", db);
    } catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    std::cout << "Total records in column store: " << db.size() << "\n";

    // phase 3: run all the (x, y) combinations
    // x is 1 to 8 (duration in months)
    // y is 80 to 150 (min floor area in sqm)
    // because we loop x first then y, the results are automatically 
    // sorted ascending x then ascending y. exactly what the spec wants.
    std::vector<QueryResult> all_results;
    
    // reserve vector size early so we dont waste time reallocating memory.
    // 8 values for x, 71 values for y (80 to 150 inclusive).
    all_results.reserve(8 * 71); 

    for (int x = 1; x <= 8; ++x) {
        for (int y = 80; y <= 150; ++y) {
            QueryResult result;
            runQuery(db, x, y, target_year, start_month, towns, result);
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