

#include "output_writer.h"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <stdexcept>

void writeResults(const std::vector<QueryResult>& results,
                  const std::string&              matric_number) {

    const std::string filename = "ScanResult_" + matric_number + ".csv";

    std::ofstream outfile(filename);
    if (!outfile.is_open()) {
        throw std::runtime_error(
            "Cannot open output file '" + filename + "' for writing.");
    }

    // write header row exactly like how the project spec want
    outfile << "(x, y),Year,Month,Town,Block,Floor_Area,"
            << "Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter\n";

    int written = 0;
    for (const auto& r : results) {
        if (r.no_result) continue; // skip those (x,y) with no result

        char month_buf[4];
        std::snprintf(month_buf, sizeof(month_buf), "%02d",
                      static_cast<int>(r.month));

        // round off price per sqm to nearest int as per spec
        const long long ppsm_rounded =
            static_cast<long long>(r.price_per_sqm + 0.5);

        outfile << "(" << r.x << ", " << r.y << "),"
                << r.year                 << ","
                << month_buf              << ","
                << r.town                 << ","
                << r.block                << ","
                << r.floor_area           << ","
                << r.flat_model           << ","
                << r.lease_commence_date  << ","
                << ppsm_rounded           << "\n";
        ++written;
    }

    outfile.close();

    std::cout << "Output written to : " << filename << "\n";
    std::cout << "Valid (x,y) pairs : " << written  << "\n";
}