/*
 * core in-memory column storage structure.
 *
 * Architecture:
 * every attribute in the resale price dataset gets its own vector.
 * we maintain strict parallel alignment so index i across all columns
 * always points to the same logical record.
 * * 
 * this is the whole point of a column store. we dont have a Row object.
 * instead of: rows[i] = { year=2017, month=6, town="TAMPINES", price=420000 }
 * we do:
 * col_month_year[i] = 2017
 * col_month_month[i] = 6
 * col_town[i] = "TAMPINES"
 * col_resale_price[i] = 420000
 *
 * Data Type choices (trying to save memory here so we dont blow up the RAM):
 * col_month_year : uint16_t for 4 digit year (2 bytes instead of 4)
 * col_month_month: uint8_t since value is only 1 to 12
 * col_floor_area : uint16_t fits max area fine
 * col_lease_commence_date: uint16_t same reasoning as year
 * col_resale_price: uint32_t fits 1.5M SGD safely 
 * String columns : std::string, skip encoding for now just keep simple
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

/*
 * ColumnStore
 * main in memory db struct. 
 */
struct ColumnStore {

    // Month originally "YYYY-MM", split it during ingestion
    // so we dont keep doing expensive string parse during queries.
    // note: dataset is not sorted by month 
    std::vector<uint16_t> col_month_year;   // eg 2017
    std::vector<uint8_t>  col_month_month;  // eg 6 for june

    // Town eg "JURONG WEST"
    // note: records from same town not clustered together
    std::vector<std::string> col_town;

    // Block eg "123A" (can have letters so must be string)
    std::vector<std::string> col_block;

    // Street Name
    std::vector<std::string> col_street_name;

    // Flat Type eg "3 ROOM", "EXECUTIVE"
    std::vector<std::string> col_flat_type;

    // Flat Model eg "Standard", "Improved"
    std::vector<std::string> col_flat_model;

    // Storey Range eg "10 TO 12". just keep as string first
    std::vector<std::string> col_storey_range;

    // Floor area in sqm
    std::vector<uint16_t> col_floor_area;

    // Lease commence year eg 1995
    std::vector<uint16_t> col_lease_commence_date;

    // Resale price in SGD
    std::vector<uint32_t> col_resale_price;

    // helper methods
    
    // return total records stored 
    std::size_t size() const;

    // clear data and free memory
    void clear();
};