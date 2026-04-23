

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "column_store.h"

// query result struct


struct QueryResult {
    int      x            = 0;
    int      y            = 0;
    uint16_t year         = 0;
    uint8_t  month        = 0;
    std::string town;
    std::string block;
    uint16_t floor_area          = 0;
    std::string flat_model;
    uint16_t lease_commence_date = 0;
    double   price_per_sqm       = 0.0; // Resale_Price / Floor_Area
    bool     no_result           = false; // true if nothing matches or min > 4725
    std::size_t local_idx = 0;
};

// helpers to get query params


std::vector<std::string> buildTownList(const std::string& matric_number);


std::vector<uint8_t> buildTownBitmapMask(const ColumnStore& db,
                                         const std::vector<std::string>& towns);


void deriveQueryParams(const std::string& matric_number,
                       uint16_t& target_year, uint8_t& start_month);

// core execution


void runQuery(const ColumnStore&              db,
              int                             x,
              int                             y,
              uint16_t                        target_year,
              uint8_t                         start_month,
              const std::vector<std::string>& towns,
              QueryResult&                    result,
              const std::vector<uint8_t>*     precomputed_town_mask = nullptr);


std::vector<std::vector<MinEntry>> buildCumulativeTable(
    const ColumnStore&              db,
    uint16_t                        target_year,
    uint8_t                         start_month,
    const std::vector<std::string>& towns
);


void runAllQueriesChunked(const ColumnStore&              base_db,
                          uint16_t                        target_year,
                          uint8_t                         start_month,
                          const std::vector<std::string>& towns,
                          std::vector<QueryResult>&       results);