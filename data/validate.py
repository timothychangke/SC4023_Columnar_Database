#!/usr/bin/env python3
"""
validate.py
quick python script to double check your ScanResult CSV against the raw data.
no pandas, just standard python so you dont need to pip install anything.
replicates the exact c++ query logic so we can tell if the "always 568 pairs" issue
is an actual bug in your code or just how the dataset naturally is for your matric number.

Usage:
    python3 validate.py <MatricNumber> <path/to/ResalePricesSingapore.csv> [<path/to/ScanResult.csv>]

Examples:
    # just run the python logic and print stats
    python3 validate.py U1234567A ../data/ResalePricesSingapore.csv

    # run and compare against your c++ output
    python3 validate.py U1234567A ../data/ResalePricesSingapore.csv ScanResult_U1234567A.csv
"""

import sys
import csv
import os

# mapping logic - exactly mirrors query_engine.cpp

TOWN_MAP = {
    0: "BEDOK",
    1: "BUKIT PANJANG",
    2: "CLEMENTI",
    3: "CHOA CHU KANG",
    4: "HOUGANG",
    5: "JURONG WEST",
    6: "PASIR RIS",
    7: "TAMPINES",
    8: "WOODLANDS",
    9: "YISHUN",
}

YEAR_MAP = {
    0: 2020, 1: 2021, 2: 2022, 3: 2023, 4: 2024,
    5: 2015, 6: 2016, 7: 2017, 8: 2018, 9: 2019,
}

MONTH_ABBR = {
    "jan": 1,  "feb": 2,  "mar": 3,  "apr": 4,
    "may": 5,  "jun": 6,  "jul": 7,  "aug": 8,
    "sep": 9,  "oct": 10, "nov": 11, "dec": 12,
}


def build_town_list(matric: str) -> list:
    # extract unique digits to get the town list
    seen = set()
    towns = []
    for c in matric:
        if c.isdigit():
            d = int(c)
            if d not in seen:
                seen.add(d)
                towns.append(TOWN_MAP[d])
    return towns


def derive_query_params(matric: str):
    # grab the last two digits of the string for year and month.
    # ignoring letters naturally.
    digits = [c for c in reversed(matric) if c.isdigit()]
    if len(digits) < 2:
        raise ValueError(f"Need at least 2 digits in matric, got: {matric!r}")
    last        = int(digits[0])
    second_last = int(digits[1])
    year        = YEAR_MAP[last]
    month       = 10 if second_last == 0 else second_last
    return year, month


def parse_month_field(s: str):
    # parse 'MMM-YY' to year and month ints. matches our C++ parser.
    if len(s) < 6 or s[3] != '-':
        raise ValueError(f"Bad month field: {s!r}")
    abbr = s[:3].lower()
    if abbr not in MONTH_ABBR:
        raise ValueError(f"Unknown month abbr: {abbr!r}")
    month = MONTH_ABBR[abbr]
    year  = 2000 + int(s[4:])
    return year, month


# csv ingestion - mirrors loadCSV 

def load_csv(filepath: str):
    # read raw csv and dump into dict of arrays.
    # doing dynamic column detection here just like the c++ side so 
    # the extra datagov.sg columns dont break the index offsets.
    cols = {
        "year": [], "month": [], "town": [], "block": [],
        "street_name": [], "flat_type": [], "flat_model": [],
        "storey_range": [], "floor_area": [], "lease_cd": [], "price": [],
    }
    skipped = 0
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = [h.strip() for h in next(reader)]

        # find column index dynamically
        col_idx = {}
        for i, h in enumerate(header):
            col_idx[h] = i
        required = ["month", "town", "flat_type", "block", "street_name",
                    "storey_range", "floor_area_sqm", "flat_model",
                    "lease_commence_date", "resale_price"]
        for r in required:
            if r not in col_idx:
                raise ValueError(f"Missing required column: {r!r}")

        n_fields = len(header)
        for lineno, row in enumerate(reader, start=2):
            if len(row) != n_fields:
                skipped += 1
                continue
            try:
                yr, mo = parse_month_field(row[col_idx["month"]].strip())
                if yr < 2015 or yr > 2025:
                    skipped += 1
                    continue
                area  = int(float(row[col_idx["floor_area_sqm"]].strip()))
                lease = int(row[col_idx["lease_commence_date"]].strip())
                price = int(row[col_idx["resale_price"]].strip())
            except (ValueError, IndexError):
                skipped += 1
                continue

            cols["year"].append(yr)
            cols["month"].append(mo)
            cols["town"].append(row[col_idx["town"]].strip())
            cols["block"].append(row[col_idx["block"]].strip())
            cols["street_name"].append(row[col_idx["street_name"]].strip())
            cols["flat_type"].append(row[col_idx["flat_type"]].strip())
            cols["flat_model"].append(row[col_idx["flat_model"]].strip())
            cols["storey_range"].append(row[col_idx["storey_range"]].strip())
            cols["floor_area"].append(area)
            cols["lease_cd"].append(lease)
            cols["price"].append(price)

    print(f"  Loaded {len(cols['year'])} records, skipped {skipped}")
    return cols


# core query simulation

def run_query(cols, x, y, target_year, start_month, towns):
    # simulate the columnar scan loop. 
    end_month = min(start_month + x - 1, 12)
    town_set  = set(towns)

    min_ppsm = float("inf")
    best_i   = None

    n = len(cols["year"])
    for i in range(n):
        # apply filters and skip bad records early
        if cols["year"][i]  != target_year:              continue
        m = cols["month"][i]
        if m < start_month or m > end_month:              continue
        if cols["town"][i]  not in town_set:              continue
        if cols["floor_area"][i] < y:                     continue

        ppsm = cols["price"][i] / cols["floor_area"][i]
        if ppsm < min_ppsm:
            min_ppsm = ppsm
            best_i   = i

    # check if result valid
    if best_i is None or min_ppsm > 4725.0:
        return None 

    return {
        "x": x, "y": y,
        "year":  cols["year"][best_i],
        "month": cols["month"][best_i],
        "town":  cols["town"][best_i],
        "block": cols["block"][best_i],
        "floor_area":  cols["floor_area"][best_i],
        "flat_model":  cols["flat_model"][best_i],
        "lease_cd":    cols["lease_cd"][best_i],
        "ppsm_raw":    min_ppsm,
        "ppsm_rounded": int(min_ppsm + 0.5),
    }


# helpers for comparing the output

def load_scan_result(filepath: str) -> dict:
    # load your c++ output so we can compare.
    # 
    # NOTE: using utf-8-sig here and doing defensive stripping on the header key.
    # sometimes the csv gets prepended with a BOM or stray whitespace depending on
    # how you compiled/ran the c++ code, which messes up python dict keys.
    results = {}
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            xy_key = next(
                (k for k in row if k.strip().startswith("(x")),
                next(iter(row))
            )
            xy = row[xy_key].strip().strip("()")
            parts = [p.strip() for p in xy.split(",")]
            key = (int(parts[0]), int(parts[1]))
            results[key] = row
    return results


def print_query_stats(cols, target_year, start_month, towns, x_range=(1,8), y_range=(80,150)):
    # prints a funnel to see where all the records are getting dropped.
    # good for debugging if you get 0 valid pairs.
    town_set = set(towns)
    n = len(cols["year"])

    pass_year  = sum(1 for i in range(n) if cols["year"][i] == target_year)
    pass_town  = sum(1 for i in range(n)
                     if cols["year"][i] == target_year
                     and cols["town"][i] in town_set)
    end_month8 = min(start_month + 8 - 1, 12)
    pass_month = sum(1 for i in range(n)
                     if cols["year"][i]  == target_year
                     and cols["town"][i] in town_set
                     and start_month <= cols["month"][i] <= end_month8)
    pass_area  = sum(1 for i in range(n)
                     if cols["year"][i]  == target_year
                     and cols["town"][i] in town_set
                     and start_month <= cols["month"][i] <= end_month8
                     and cols["floor_area"][i] >= y_range[0])

    print(f"\n--- Predicate funnel (widest window x=8, y={y_range[0]}) ---")
    print(f"  Total records         : {n}")
    print(f"  After year={target_year}         : {pass_year}")
    print(f"  After town filter     : {pass_town}")
    print(f"  After month [{start_month},{end_month8}]   : {pass_month}")
    print(f"  After area>={y_range[0]}        : {pass_area}")
    if pass_area > 0:
        ppsm_vals = [cols["price"][i] / cols["floor_area"][i]
                     for i in range(n)
                     if cols["year"][i]  == target_year
                     and cols["town"][i] in town_set
                     and start_month <= cols["month"][i] <= end_month8
                     and cols["floor_area"][i] >= y_range[0]]
        print(f"  Min PPSM in that pool : {min(ppsm_vals):.2f}")
        print(f"  Max PPSM in that pool : {max(ppsm_vals):.2f}")
        below_threshold = sum(1 for p in ppsm_vals if p <= 4725)
        print(f"  Records with ppsm<=4725 : {below_threshold}")
    print()


# main runner

def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    matric    = sys.argv[1]
    csv_path  = sys.argv[2]
    scan_path = sys.argv[3] if len(sys.argv) > 3 else None

    print(f"\nMatriculation number : {matric}")
    target_year, start_month = derive_query_params(matric)
    towns = build_town_list(matric)
    print(f"Target year          : {target_year}")
    print(f"Start month          : {start_month}")
    print(f"Target towns         : {', '.join(towns)}")

    print(f"\nLoading CSV: {csv_path}")
    cols = load_csv(csv_path)

    # run the diagnostic funnel
    print_query_stats(cols, target_year, start_month, towns)

    print("Running all (x, y) queries...")
    valid_pairs = []
    for x in range(1, 9):
        for y in range(80, 151):
            r = run_query(cols, x, y, target_year, start_month, towns)
            if r:
                valid_pairs.append(r)

    print(f"\nPython computed {len(valid_pairs)} valid (x,y) pairs")

    if len(valid_pairs) == 8 * 71:
        print("\nWARNING: ALL 568 pairs are valid.")
        print("   This is mathematically possible but very unusual.")
        print("   Check the predicate funnel above — if 'After year' is large")
        print("   and min PPSM is well below 4725, your data genuinely produces this.")
    elif len(valid_pairs) == 0:
        print("\nWARNING: ZERO valid pairs found.")
        print("   Check the predicate funnel above for which filter eliminates everything.")

    print("\nFirst 5 valid pairs (Python reference):")
    for r in valid_pairs[:5]:
        print(f"  ({r['x']}, {r['y']}) -> {r['year']}/{r['month']:02d} "
              f"{r['town']} blk {r['block']} area={r['floor_area']} "
              f"ppsm={r['ppsm_rounded']}")

    # diff checking against your c++ output
    if scan_path and os.path.exists(scan_path):
        print(f"\nComparing against: {scan_path}")
        scan = load_scan_result(scan_path)
        print(f"  Your program output : {len(scan)} valid pairs")
        print(f"  Python reference    : {len(valid_pairs)} valid pairs")

        py_dict = {(r["x"], r["y"]): r for r in valid_pairs}

        mismatches = []
        missing_in_yours = []
        extra_in_yours   = []

        for key in py_dict:
            if key not in scan:
                missing_in_yours.append(key)

        for key in scan:
            if key not in py_dict:
                extra_in_yours.append(key)

        for key in py_dict:
            if key not in scan:
                continue
            ref = py_dict[key]
            got = scan[key]
            issues = []
            if str(ref["year"])         != got["Year"].strip():
                issues.append(f"year: expected {ref['year']} got {got['Year'].strip()}")
            if f"{ref['month']:02d}"    != got["Month"].strip():
                issues.append(f"month: expected {ref['month']:02d} got {got['Month'].strip()}")
            if ref["town"]              != got["Town"].strip():
                issues.append(f"town: expected {ref['town']!r} got {got['Town'].strip()!r}")
            if str(ref["ppsm_rounded"]) != got["Price_Per_Square_Meter"].strip():
                issues.append(f"ppsm: expected {ref['ppsm_rounded']} "
                              f"got {got['Price_Per_Square_Meter'].strip()}")
            if str(ref["floor_area"])   != got["Floor_Area"].strip():
                issues.append(f"area: expected {ref['floor_area']} got {got['Floor_Area'].strip()}")
            if issues:
                mismatches.append((key, issues))

        if not mismatches and not missing_in_yours and not extra_in_yours:
            print("\nPERFECT MATCH — your output agrees with the Python reference on all pairs.")
        else:
            if missing_in_yours:
                print(f"\n  {len(missing_in_yours)} pairs in Python but MISSING from your output:")
                for k in missing_in_yours[:10]:
                    print(f"     {k}")
            if extra_in_yours:
                print(f"\n{len(extra_in_yours)} pairs in your output NOT in Python reference:")
                for k in extra_in_yours[:10]:
                    print(f"     {k}")
            if mismatches:
                print(f"\n{len(mismatches)} pairs with WRONG field values:")
                for key, issues in mismatches[:10]:
                    print(f"     {key}: {'; '.join(issues)}")
    else:
        # just write the reference output if no scan path provided
        out_name = f"PythonRef_{matric}.csv"
        print(f"\nWriting Python reference output to: {out_name}")
        with open(out_name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["(x, y)", "Year", "Month", "Town", "Block",
                        "Floor_Area", "Flat_Model", "Lease_Commence_Date",
                        "Price_Per_Square_Meter"])
            for r in valid_pairs:
                w.writerow([
                    f"({r['x']}, {r['y']})",
                    r["year"], f"{r['month']:02d}",
                    r["town"], r["block"],
                    r["floor_area"], r["flat_model"],
                    r["lease_cd"], r["ppsm_rounded"],
                ])
        print(f"  Written {len(valid_pairs)} rows.")


if __name__ == "__main__":
    main()