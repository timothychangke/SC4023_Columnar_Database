/*
 * * handles writing the query results into our output csv file.
 * * file format based on the project spec:
 * Filename: ScanResult_<MatricNum>.csv
 * Header: (x, y),Year,Month,Town,Block,Floor_Area,Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter
 * Note: we need to order by ascending x, then ascending y.
 * if got no valid result for the (x,y) pair, just skip it dont output.
 * Price_Per_Square_Meter must round to nearest int.
 */

#pragma once

#include <string>
#include <vector>
#include "query_engine.h"

/*
 * writeResults
 * * dump all the valid QueryResult stuff into ScanResult_<matric_number>.csv
 * * Make sure the results vector is already sorted (x ascending then y) before
 * calling this function. runQuery should settle the sorting and just append.
 */
void writeResults(const std::vector<QueryResult>& results,
                  const std::string&              matric_number);