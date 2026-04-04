/*
 * Separate Binary Column Files — I/O implementation.
 *
 * Purpose:
 *   - Write each ColumnStore column as its own binary file (one-time conversion)
 *   - Load only the filter columns needed for query scanning (selective read)
 *   - Lazy load materialisation columns for just the winning row (random access)

 */

#include "column_file_io.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static void writeDictionary(std::ofstream& f, const DictionaryEncoder& dict) {
    const uint32_t num_entries = static_cast<uint32_t>(dict.id_to_str.size());
    f.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));

    for (uint32_t i = 0; i < num_entries; ++i) {
        const std::string& s = dict.id_to_str[i];
        const uint32_t len = static_cast<uint32_t>(s.size());
        f.write(reinterpret_cast<const char*>(&len), sizeof(len));
        f.write(s.data(), len);
    }
}

/*
 * readDictionary
 * Deserialise a DictionaryEncoder from a binary stream.
 * Reconstructs both id_to_str and str_to_id with the same ID assignments
 * that were used during writing.
 */
static void readDictionary(std::ifstream& f, DictionaryEncoder& dict) {
    dict.clear();

    uint32_t num_entries = 0;
    f.read(reinterpret_cast<char*>(&num_entries), sizeof(num_entries));

    dict.id_to_str.reserve(num_entries);
    for (uint32_t i = 0; i < num_entries; ++i) {
        uint32_t len = 0;
        f.read(reinterpret_cast<char*>(&len), sizeof(len));

        std::string s(len, '\0');
        f.read(s.data(), len);

        // encode() assigns sequential IDs, so calling it in order
        // reproduces the original mapping exactly.
        dict.encode(s);
    }
}

static void writeMeta(const ColumnStore& db, const std::string& filepath) {
    std::ofstream f(filepath, std::ios::binary);
    if (!f.is_open()) {
        throw std::runtime_error("Cannot open meta file for writing: " + filepath);
    }

    const uint32_t N = static_cast<uint32_t>(db.size());
    f.write(reinterpret_cast<const char*>(&N), sizeof(N));

    // flags byte: encode which optional columns are present
    // bit 0 = dict_encoding (A1), bit 1 = precomputed_ppsm (A4)
    uint8_t flags = 0;
    if (db.use_dict_encoding)    flags |= 0x01;
    if (db.use_precomputed_ppsm) flags |= 0x02;
    f.write(reinterpret_cast<const char*>(&flags), sizeof(flags));

    // always write all four dictionaries — if dict_encoding is off,
    // they'll just have 0 entries, which is fine.
    writeDictionary(f, db.dict_town);
    writeDictionary(f, db.dict_flat_type);
    writeDictionary(f, db.dict_flat_model);
    writeDictionary(f, db.dict_street_name);
}


static void readMeta(const std::string& filepath, ColumnStore& db) {
    std::ifstream f(filepath, std::ios::binary);
    if (!f.is_open()) {
        throw std::runtime_error("Cannot open meta file for reading: " + filepath);
    }

    uint32_t N = 0;
    f.read(reinterpret_cast<char*>(&N), sizeof(N));
    db.total_rows = N;

    uint8_t flags = 0;
    f.read(reinterpret_cast<char*>(&flags), sizeof(flags));
    db.use_dict_encoding    = (flags & 0x01) != 0;
    db.use_precomputed_ppsm = (flags & 0x02) != 0;

    readDictionary(f, db.dict_town);
    readDictionary(f, db.dict_flat_type);
    readDictionary(f, db.dict_flat_model);
    readDictionary(f, db.dict_street_name);
}

void writeColumnFiles(const ColumnStore& db, const std::string& dir) {
    // create output directory if it doesn't exist
    std::filesystem::create_directories(dir);

    const uint32_t N = static_cast<uint32_t>(db.size());
    if (N == 0) {
        std::cerr << "Warning: writeColumnFiles called with empty ColumnStore\n";
        return;
    }

    // ── Helper lambda: write a fixed-width numeric column ──
    // Format: [uint32_t row_count] [element × row_count]
    auto writeNumericCol = [&](const auto& vec, const std::string& name) {
        std::ofstream f(dir + "/" + name, std::ios::binary);
        if (!f.is_open()) {
            throw std::runtime_error("Cannot open column file for writing: " + dir + "/" + name);
        }
        f.write(reinterpret_cast<const char*>(&N), sizeof(N));
        f.write(reinterpret_cast<const char*>(vec.data()),
                vec.size() * sizeof(vec[0]));
    };

    // ── Helper lambda: write a variable-length string column ──
    // Format: [uint32_t row_count]
    //         [uint32_t offset_table[row_count + 1]]
    //         [char[] packed_strings]
    auto writeStringCol = [&](const std::vector<std::string>& col,
                              const std::string& name) {
        std::ofstream f(dir + "/" + name, std::ios::binary);
        if (!f.is_open()) {
            throw std::runtime_error("Cannot open column file for writing: " + dir + "/" + name);
        }
        f.write(reinterpret_cast<const char*>(&N), sizeof(N));

        // build offset table: offset[i] = byte position where string i starts
        // offset[N] = total byte length of all strings concatenated
        std::vector<uint32_t> offsets(N + 1);
        uint32_t running = 0;
        for (uint32_t i = 0; i < N; ++i) {
            offsets[i] = running;
            running += static_cast<uint32_t>(col[i].size());
        }
        offsets[N] = running;

        // write offset table
        f.write(reinterpret_cast<const char*>(offsets.data()),
                offsets.size() * sizeof(uint32_t));

        // write packed string data
        for (const auto& s : col) {
            f.write(s.data(), s.size());
        }
    };

    // ── Write all numeric columns (always present) ──
    writeNumericCol(db.col_month_year,          "month_year.col");
    writeNumericCol(db.col_month_month,         "month_month.col");
    writeNumericCol(db.col_floor_area,          "floor_area.col");
    writeNumericCol(db.col_resale_price,        "resale_price.col");
    writeNumericCol(db.col_lease_commence_date, "lease_commence_date.col");

    // ── Write optional numeric columns (only if the flag was on during CSV load) ──
    if (db.use_dict_encoding) {
        writeNumericCol(db.col_town_encoded,        "town_encoded.col");
        writeNumericCol(db.col_flat_type_encoded,   "flat_type_encoded.col");
        writeNumericCol(db.col_flat_model_encoded,  "flat_model_encoded.col");
        writeNumericCol(db.col_street_name_encoded, "street_name_encoded.col");
    }
    if (db.use_precomputed_ppsm) {
        writeNumericCol(db.col_price_per_sqm, "price_per_sqm.col");
    }

    // ── Write all string columns (needed for lazy materialisation) ──
    writeStringCol(db.col_town,         "town.col");
    writeStringCol(db.col_block,        "block.col");
    writeStringCol(db.col_flat_model,   "flat_model.col");
    writeStringCol(db.col_street_name,  "street_name.col");
    writeStringCol(db.col_flat_type,    "flat_type.col");
    writeStringCol(db.col_storey_range, "storey_range.col");

    // ── Write meta file (row count + flags + dictionaries) ──
    writeMeta(db, dir + "/meta.col");

    std::cout << "Column files written to: " << dir << "\n";
    std::cout << "  Row count: " << N << "\n";
    std::cout << "  Dict encoding: " << (db.use_dict_encoding ? "yes" : "no") << "\n";
    std::cout << "  Precomputed PPSM: " << (db.use_precomputed_ppsm ? "yes" : "no") << "\n";
}


void loadColumnFiles(const std::string& dir, ColumnStore& db) {
    // read meta first to get row count + dictionaries + flags
    readMeta(dir + "/meta.col", db);
    const std::size_t N = db.total_rows;

    if (N == 0) {
        std::cout << "Column files loaded: 0 rows (empty dataset)\n";
        return;
    }

    // ── Helper lambda: read a fixed-width numeric column ──
    auto readNumericCol = [&](auto& vec, const std::string& name) {
        std::ifstream f(dir + "/" + name, std::ios::binary);
        if (!f.is_open()) {
            throw std::runtime_error("Cannot open column file for reading: " + dir + "/" + name);
        }

        uint32_t file_N = 0;
        f.read(reinterpret_cast<char*>(&file_N), sizeof(file_N));

        if (static_cast<std::size_t>(file_N) != N) {
            throw std::runtime_error(
                "Row count mismatch in " + name + ": meta says " +
                std::to_string(N) + " but file says " + std::to_string(file_N));
        }

        vec.resize(file_N);
        f.read(reinterpret_cast<char*>(vec.data()),
               file_N * sizeof(vec[0]));
    };

    auto readStringCol = [&](std::vector<std::string>& col, const std::string& filepath) {
        std::ifstream f(filepath, std::ios::binary);
        if (!f.is_open()) {
            throw std::runtime_error("Cannot open column file for reading: " + filepath);
        }

        uint32_t file_N = 0;
        f.read(reinterpret_cast<char*>(&file_N), sizeof(file_N));

        if (static_cast<std::size_t>(file_N) != N) {
            throw std::runtime_error(
                "Row count mismatch in " + filepath + ": meta says " +
                std::to_string(N) + " but file says " + std::to_string(file_N));
        }

        // read the offset table
        std::vector<uint32_t> offsets(file_N + 1);
        f.read(reinterpret_cast<char*>(offsets.data()),
               offsets.size() * sizeof(uint32_t));

        // read the packed string blob
        const uint32_t total_string_bytes = offsets[file_N];
        std::vector<char> blob(total_string_bytes);
        f.read(blob.data(), total_string_bytes);

        // unpack into individual strings
        col.resize(file_N);
        for (uint32_t i = 0; i < file_N; ++i) {
            const uint32_t start = offsets[i];
            const uint32_t len   = offsets[i + 1] - start;
            col[i].assign(blob.data() + start, len);
        }
    };

    // Always needed for filtering
    readNumericCol(db.col_month_year,   "month_year.col");
    readNumericCol(db.col_month_month,  "month_month.col");
    readNumericCol(db.col_floor_area,   "floor_area.col");
    readNumericCol(db.col_resale_price, "resale_price.col");

    // Town column: load either the encoded version (A1) or the raw string version
    if (db.use_dict_encoding) {
        readNumericCol(db.col_town_encoded, "town_encoded.col");
        // dictionaries were already restored by readMeta()
    } else {
        // need full town strings for filtering (string == string)
        readStringCol(db.col_town, dir + "/town.col");
    }

    // Precomputed PPSM (A4): only load if the flag is on
    if (db.use_precomputed_ppsm) {
        readNumericCol(db.col_price_per_sqm, "price_per_sqm.col");
    }

    // ── Materialisation columns are NOT loaded here ──
    // block, flat_model, lease_commence_date, street_name, flat_type,
    // storey_range will be loaded lazily for just the winning row index
    // via loadStringAt() / loadUint16At().

    std::cout << "Column files loaded: " << N << " rows from " << dir << "\n";

    // ── B1: build zone maps if enabled (normally done inside loadCSV) ──
    if (db.use_zone_maps) {
        const std::size_t num_chunks = (N + ZONE_CHUNK_SIZE - 1) / ZONE_CHUNK_SIZE;

        auto buildZoneMap16 = [&](const std::vector<uint16_t>& col) -> ZoneMap {
            ZoneMap zm;
            zm.chunks.resize(num_chunks);
            for (std::size_t i = 0; i < N; ++i) {
                std::size_t c = i / ZONE_CHUNK_SIZE;
                uint32_t val = static_cast<uint32_t>(col[i]);
                if (val < zm.chunks[c].min_val) zm.chunks[c].min_val = val;
                if (val > zm.chunks[c].max_val) zm.chunks[c].max_val = val;
            }
            return zm;
        };
        auto buildZoneMap8 = [&](const std::vector<uint8_t>& col) -> ZoneMap {
            ZoneMap zm;
            zm.chunks.resize(num_chunks);
            for (std::size_t i = 0; i < N; ++i) {
                std::size_t c = i / ZONE_CHUNK_SIZE;
                uint32_t val = static_cast<uint32_t>(col[i]);
                if (val < zm.chunks[c].min_val) zm.chunks[c].min_val = val;
                if (val > zm.chunks[c].max_val) zm.chunks[c].max_val = val;
            }
            return zm;
        };
        auto buildZoneMap32 = [&](const std::vector<uint32_t>& col) -> ZoneMap {
            ZoneMap zm;
            zm.chunks.resize(num_chunks);
            for (std::size_t i = 0; i < N; ++i) {
                std::size_t c = i / ZONE_CHUNK_SIZE;
                uint32_t val = col[i];
                if (val < zm.chunks[c].min_val) zm.chunks[c].min_val = val;
                if (val > zm.chunks[c].max_val) zm.chunks[c].max_val = val;
            }
            return zm;
        };

        db.zm_floor_area   = buildZoneMap16(db.col_floor_area);
        db.zm_resale_price = buildZoneMap32(db.col_resale_price);
        db.zm_month_year   = buildZoneMap16(db.col_month_year);
        db.zm_month_month  = buildZoneMap8(db.col_month_month);

        std::cout << "  Zone maps built: " << num_chunks
                  << " chunks of " << ZONE_CHUNK_SIZE << " rows each\n";
    }

    std::cout << "  Columns loaded: month_year, month_month, floor_area, resale_price"
              << (db.use_dict_encoding ? ", town_encoded" : ", town")
              << (db.use_precomputed_ppsm ? ", price_per_sqm" : "") << "\n";
    std::cout << "  Materialisation columns: deferred (lazy load)\n";
}


std::string loadStringAt(const std::string& filepath, std::size_t idx) {
    std::ifstream f(filepath, std::ios::binary);
    if (!f.is_open()) {
        throw std::runtime_error("Cannot open column file for lazy read: " + filepath);
    }

    // read row count
    uint32_t N = 0;
    f.read(reinterpret_cast<char*>(&N), sizeof(N));

    if (idx >= N) {
        throw std::out_of_range(
            "loadStringAt: index " + std::to_string(idx) +
            " out of range (file has " + std::to_string(N) + " rows)");
    }

    uint32_t off_start = 0, off_end = 0;
    f.seekg(static_cast<std::streamoff>(sizeof(uint32_t) + idx * sizeof(uint32_t)));
    f.read(reinterpret_cast<char*>(&off_start), sizeof(off_start));
    f.read(reinterpret_cast<char*>(&off_end), sizeof(off_end));

    // The string data region starts after the full offset table:
    //   data_start = sizeof(uint32_t) + (N + 1) * sizeof(uint32_t)
    const std::size_t data_start = sizeof(uint32_t) + (static_cast<std::size_t>(N) + 1) * sizeof(uint32_t);
    const std::size_t len = off_end - off_start;

    std::string result(len, '\0');
    f.seekg(static_cast<std::streamoff>(data_start + off_start));
    f.read(result.data(), static_cast<std::streamsize>(len));

    return result;
}



uint16_t loadUint16At(const std::string& filepath, std::size_t idx) {
    std::ifstream f(filepath, std::ios::binary);
    if (!f.is_open()) {
        throw std::runtime_error("Cannot open column file for lazy read: " + filepath);
    }

    // read row count
    uint32_t N = 0;
    f.read(reinterpret_cast<char*>(&N), sizeof(N));

    if (idx >= N) {
        throw std::out_of_range(
            "loadUint16At: index " + std::to_string(idx) +
            " out of range (file has " + std::to_string(N) + " rows)");
    }

    // seek past the row_count header to the target element
    //   byte position = sizeof(uint32_t) + idx * sizeof(uint16_t)
    f.seekg(static_cast<std::streamoff>(sizeof(uint32_t) + idx * sizeof(uint16_t)));

    uint16_t value = 0;
    f.read(reinterpret_cast<char*>(&value), sizeof(value));

    return value;
}