

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <limits>


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


struct MinEntry { 
    bool has = false; 
    double ppsm = 0.0; 
    std::size_t idx = 0; 
};

struct TownPartition {
    std::size_t begin = 0; // inclusive
    std::size_t end   = 0; // exclusive
    bool valid = false;
};


static constexpr std::size_t ZONE_CHUNK_SIZE = 1024; // rows per chunk

struct ZoneMapEntry {
    uint32_t min_val = std::numeric_limits<uint32_t>::max();
    uint32_t max_val = 0;
};

struct ZoneMap {
    std::vector<ZoneMapEntry> chunks;

    // check if a chunk CAN contain values >= threshold
    // returns false → safe to skip the entire chunk
    bool chunkMayContainGEQ(std::size_t chunk_idx, uint32_t threshold) const {
        return chunks[chunk_idx].max_val >= threshold;
    }

    // check if a chunk CAN contain values <= threshold
    bool chunkMayContainLEQ(std::size_t chunk_idx, uint32_t threshold) const {
        return chunks[chunk_idx].min_val <= threshold;
    }

    // check if a chunk CAN contain values == target
    bool chunkMayContainEQ(std::size_t chunk_idx, uint32_t target) const {
        return chunks[chunk_idx].min_val <= target &&
               chunks[chunk_idx].max_val >= target;
    }

    std::size_t numChunks() const { return chunks.size(); }
};


struct ColumnStore {

    bool use_dict_encoding = false;

    bool use_reuse = false;
    std::vector<std::vector<MinEntry>> cum_table; // 2D container matrix to store precomputed cumulative min ppsm for all (x,y) combinations when reuse is enabled. 

    bool use_precomputed_ppsm = false;

    bool use_int_multiply = false;

    bool use_predicate_reorder = false;

    // Pre-computed Price Per SqM (only populated when use_precomputed_ppsm=true)
    // Stored as double to preserve precision for min-comparison.
    std::vector<double> col_price_per_sqm;


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

    bool use_bitmap_index_town = false;
    std::vector<std::vector<bool>> town_bitmaps;
    std::unordered_map<std::string, uint16_t> town_bitmap_lookup;

    // observability counters (updated by runQuery)
    mutable std::size_t town_bitmap_lookups = 0;        // number of bitmap vectors OR-ed into masks
    mutable std::size_t town_bitmap_evaluations = 0;    // rows checked against town bitmap mask
    mutable std::size_t rows_eliminated_by_bitmap = 0;  // rows skipped by bitmap mask

    bool use_zone_maps = false;
    
    bool use_late_materialise = false;

    bool use_columnar_files = false;

    // Path to the directory containing .col files
    std::string column_dir = "data/columns";

    // Row count (read from meta.col, used to pre-size vectors)
    std::size_t total_rows = 0;
    
    bool use_chunked_io = false;

    // User-facing memory budget in bytes. Default 50 MB.
    std::size_t memory_budget_bytes = 50 * 1024 * 1024;

    // Computed at query time from memory_budget_bytes + enabled flags.
    std::size_t io_chunk_rows = 0;

    // Observability counters for the report.
    // Mutable so runAllQueriesChunked can update them via a const ref.
    mutable std::size_t io_chunks_loaded = 0;
    mutable std::size_t io_bytes_read    = 0;
    
    bool use_mmap_io = false;

    bool use_town_partitioning = false;

    // When town_partitioning is active, this records which partition dirs
    // were loaded (for diagnostic / instrumentation purposes).
    std::vector<std::string> loaded_partition_dirs;

    // Storage for mmap handles so the destructor can munmap them.
    struct MappedRegion {
        void*       addr   = nullptr;
        std::size_t length = 0;
        int         fd     = -1;
    };
    std::vector<MappedRegion> mmap_regions;

    // Zone maps for filterable numeric columns


    // Zone maps for filterable numeric columns
    ZoneMap zm_floor_area;       // used for: floor_area >= y
    ZoneMap zm_resale_price;     // used for: price/sqm <= 4725 (via price <= 4725*area)
    ZoneMap zm_month_year;       // used for: year == target_year
    ZoneMap zm_month_month;      // used for: month in [start, end]

    bool use_presorted_storage = false;

    // Town partition metadata generated when use_presorted_storage is enabled.
    // string path
    std::unordered_map<std::string, TownPartition> town_partitions;
    // dictionary path (index = town_id)
    std::vector<TownPartition> town_partitions_encoded;

    bool use_month_binary_search = false;

    bool use_rle_town = false;

    // Dictionary mode run values 
    std::vector<uint16_t> town_run_value_encoded;
    // String mode run values 
    std::vector<std::string> town_run_value;

    // Run boundaries: run k covers [town_run_start[k], town_run_start[k] + town_run_length[k])
    std::vector<uint32_t> town_run_start;
    std::vector<uint32_t> town_run_length;

    // Optional helper indexes for fast run lookup by town
    std::unordered_map<uint16_t, std::vector<uint32_t>> town_to_runs_encoded;
    std::unordered_map<std::string, std::vector<uint32_t>> town_to_runs;

    // observability counters (updated during query execution)
    mutable uint64_t town_runs_scanned = 0;
    mutable uint64_t rows_skipped_by_rle = 0;
    mutable uint64_t rows_scanned_after_rle = 0;

    // helper methods
    
    // return total records stored 
    std::size_t size() const;

    // (Re)build the town bitmap index from current row order.
    // Must be called after any row reordering 
    void rebuildTownBitmaps();
    
    // Build town run-length metadata from current row order.
    void buildTownRLE();

    // clear data and free memory
    void clear();
};