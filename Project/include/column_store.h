/*
 * core in-memory column storage structure.
 *
 * Architecture:
 * every attribute in the resale price dataset gets its own vector.
 * we maintain strict parallel alignment so index i across all columns
 * always points to the same logical record.
 * 
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
 *
 * === OPTIMISATION: Dictionary Encoding (A1) ===
 * When enabled, string columns (Town, Flat_Type, Flat_Model, Street_Name)
 * are replaced with uint16_t integer IDs during CSV ingestion.
 * A DictionaryEncoder maintains the bidirectional mapping.
 * All downstream query comparisons become int==int instead of string==string.
 * Toggle: set ColumnStore::use_dict_encoding = true before calling loadCSV.
 */

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

/*
 * DictionaryEncoder
 * maps unique strings to compact uint16_t IDs and back.
 *
 * encode("TAMPINES") -> 0  (first call assigns ID 0)
 * encode("TAMPINES") -> 0  (subsequent calls return same ID)
 * decode(0)          -> "TAMPINES"
 *
 * max 65535 unique values per column which is more than enough 
 * for town (~26), flat_type (~7), flat_model (~20), street_name (~500).
 */
struct DictionaryEncoder {
    std::unordered_map<std::string, uint16_t> str_to_id;
    std::vector<std::string>                  id_to_str;

    // encode a string to its integer ID. assigns a new ID if first time seen.
    uint16_t encode(const std::string& value) {
        auto it = str_to_id.find(value);
        if (it != str_to_id.end()) {
            return it->second;
        }
        uint16_t new_id = static_cast<uint16_t>(id_to_str.size());
        str_to_id[value] = new_id;
        id_to_str.push_back(value);
        return new_id;
    }

    // decode an integer ID back to its original string.
    const std::string& decode(uint16_t id) const {
        return id_to_str.at(id);
    }

    // lookup an existing string's ID. returns false if not found.
    bool lookup(const std::string& value, uint16_t& out_id) const {
        auto it = str_to_id.find(value);
        if (it == str_to_id.end()) return false;
        out_id = it->second;
        return true;
    }

    // how many unique values are stored
    std::size_t size() const {
        return id_to_str.size();
    }

    void clear() {
        str_to_id.clear();
        id_to_str.clear();
    }
};

/*
 * ColumnStore
 * main in memory db struct. 
 */
struct ColumnStore {

    // Dictionary encoding optimisation flag
    // This is set to enable/disable dictionary encoding.
    // When true, encoded int columns are populated instead, and dictionaries
    // are built during ingestion. the original string columns are still populated
    // so that output_writer can retrieve the original strings for the final CSV.
    bool use_dict_encoding = false;

    // Month originally "YYYY-MM", split it during ingestion
    // so we dont keep doing expensive string parse during queries.
    // note: dataset is not sorted by month 
    std::vector<uint16_t> col_month_year;   // eg 2017
    std::vector<uint8_t>  col_month_month;  // eg 6 for june

    // ---- baseline string columns ----

    // Town eg "JURONG WEST"
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

    // ---- dictionary-encoded columns (only populated when use_dict_encoding=true) ----

    std::vector<uint16_t> col_town_encoded;
    std::vector<uint16_t> col_flat_type_encoded;
    std::vector<uint16_t> col_flat_model_encoded;
    std::vector<uint16_t> col_street_name_encoded;

    // dictionaries for each encoded column
    DictionaryEncoder dict_town;
    DictionaryEncoder dict_flat_type;
    DictionaryEncoder dict_flat_model;
    DictionaryEncoder dict_street_name;

    // helper methods
    
    // return total records stored 
    std::size_t size() const;

    // clear data and free memory
    void clear();
};