/*
 * implementations for ColumnStore helpers
 */

#include "column_store.h"
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>

// return how many records we have. 
// all vectors are parallel so just checking one col size is enough.
std::size_t ColumnStore::size() const {
    return col_month_year.size();
}

void ColumnStore::rebuildTownBitmaps() {
    town_bitmaps.clear();
    town_bitmap_lookup.clear();

    town_bitmap_lookups = 0;
    town_bitmap_evaluations = 0;
    rows_eliminated_by_bitmap = 0;

    if (!use_bitmap_index_town) return;

    const std::size_t N = size();
    if (N == 0) return;

    if (use_dict_encoding) {
        std::size_t bitmap_count = dict_town.size();
        if (!col_town_encoded.empty()) {
            uint16_t max_id = *std::max_element(col_town_encoded.begin(), col_town_encoded.end());
            bitmap_count = std::max(bitmap_count, static_cast<std::size_t>(max_id) + 1);
        }
        town_bitmaps.assign(bitmap_count, std::vector<bool>(N, false));

        for (std::size_t i = 0; i < N && i < col_town_encoded.size(); ++i) {
            const uint16_t tid = col_town_encoded[i];
            if (tid >= town_bitmaps.size()) {
                town_bitmaps.resize(static_cast<std::size_t>(tid) + 1, std::vector<bool>(N, false));
            }
            town_bitmaps[tid][i] = true;
        }
        return;
    }

    uint16_t next_id = 0;
    for (std::size_t i = 0; i < N && i < col_town.size(); ++i) {
        const std::string& town = col_town[i];
        auto it = town_bitmap_lookup.find(town);
        if (it == town_bitmap_lookup.end()) {
            const uint16_t tid = next_id++;
            town_bitmap_lookup.emplace(town, tid);
            town_bitmaps.push_back(std::vector<bool>(N, false));
            it = town_bitmap_lookup.find(town);
        }
        town_bitmaps[it->second][i] = true;
    }
}

// clear everything and release the heap memory
void ColumnStore::clear() {
    col_month_year.clear();
    col_month_month.clear();
    col_town.clear();
    col_block.clear();
    col_street_name.clear();
    col_flat_type.clear();
    col_flat_model.clear();
    col_storey_range.clear();
    col_floor_area.clear();
    col_lease_commence_date.clear();
    col_resale_price.clear();
    col_price_per_sqm.clear();

    col_town_encoded.clear();
    col_flat_type_encoded.clear();
    col_flat_model_encoded.clear();
    col_street_name_encoded.clear();

    dict_town.clear();
    dict_flat_type.clear();
    dict_flat_model.clear();
    dict_street_name.clear();

    town_bitmaps.clear();
    town_bitmap_lookup.clear();
    town_bitmap_lookups = 0;
    town_bitmap_evaluations = 0;
    rows_eliminated_by_bitmap = 0;

    town_partitions.clear();
    town_partitions_encoded.clear();
    
    // zone maps
    zm_floor_area.chunks.clear();
    zm_resale_price.chunks.clear();
    zm_month_year.chunks.clear();
    zm_month_month.chunks.clear();
    
    // D2: unmap any mmap regions
    for (auto& mr : mmap_regions) {
        if (mr.addr && mr.addr != MAP_FAILED) {
            ::munmap(mr.addr, mr.length);
        }
        if (mr.fd >= 0) {
            ::close(mr.fd);
        }
    }
    mmap_regions.clear();

    column_dir = "data/columns/";
    total_rows = 0;
}