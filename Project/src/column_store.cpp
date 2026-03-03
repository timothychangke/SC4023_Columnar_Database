/*
 * implementations for ColumnStore helpers
 */

#include "column_store.h"

// return how many records we have. 
// all vectors are parallel so just checking one col size is enough.
std::size_t ColumnStore::size() const {
    return col_month_year.size();
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
}