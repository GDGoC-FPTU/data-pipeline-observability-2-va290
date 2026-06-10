"""
==============================================================
generate_data_lake.py
==============================================================
Tao mot "Data Lake" gom NHIEU file CSV voi do kho cao hon bai
goc, de stress-test ETL pipeline xu ly song song nhieu file.

Cac van de chat luong du lieu duoc cai cam (harder scenario):
   - Schema KHAC NHAU giua cac file (thu tu cot khac, thua cot,
     THIEU han cot 'category').
   - Gia (price) o nhieu dinh dang ban: "$1,200.50", "1.2e3",
     "ten dollars", "N/A", chuoi rong, so am, so 0, co khoang trang.
   - Category lon/thuong/ co khoang trang thua: " ELECTRONICS ".
   - Trung ID trong cung file VA xuyen nhieu file (global duplicate).
   - Gia tri null (None / o trong).
   - Outlier cuc doan (999999).
   - Hang rac chen ngau nhien giua khoi du lieu sach.

Chay:
   python generate_data_lake.py
Ket qua: tao thu muc ./data_lake/ chua nhieu file *.csv
==============================================================
"""

import csv
import os

OUTPUT_DIR = "data_lake"
# So dong "sach" sinh them moi file de khoi luong du lon, do duoc
# chenh lech thoi gian giua xu ly tuan tu va song song.
CLEAN_ROWS_PER_FILE = 80_000

CATEGORIES = ["electronics", "furniture", "books", "toys", "sports"]

# Cac dong rac/edge-case co dinh (deterministic) cam vao moi file.
# Format: dict theo ten cot de phu hop voi schema tung file.
MESSY_ROWS = [
    {"id": 1,    "product": "Laptop",          "price": "$1,200.50", "category": "electronics"},   # currency + comma
    {"id": 1,    "product": "Banana",          "price": "2",         "category": "fruit"},          # duplicate id trong file
    {"id": 9001, "product": "Broken Chair",    "price": "ten dollars","category": "furniture"},     # gia la chu
    {"id": 9002, "product": "Nuclear Reactor", "price": "999999",    "category": "electronics"},     # outlier
    {"id": 9003, "product": "Ghost Item",      "price": "0",         "category": ""},                # gia 0 + category rong
    {"id": 9004, "product": "Null Phone",      "price": "",          "category": None},              # null gia + category
    {"id": 9005, "product": "Refund",          "price": "-50",       "category": "books"},           # gia am
    {"id": 9006, "product": "  Spaced Mouse  ","price": "  29.99 ",  "category": " ELECTRONICS "},   # whitespace + hoa
    {"id": 9007, "product": "Sci Calc",        "price": "1.2e3",     "category": "electronics"},      # khoa hoc (1200)
    {"id": 9008, "product": "Mystery",         "price": "N/A",       "category": "misc"},            # N/A
]


def _row_for_schema(row, columns):
    """Tra ve list gia tri theo dung thu tu 'columns' cua file.
    Neu file thieu cot do (vd khong co 'category'), bo qua cot do."""
    out = []
    for col in columns:
        val = row.get(col, "")
        out.append("" if val is None else val)
    return out


def _make_file(path, columns, id_offset):
    """Sinh 1 file CSV theo schema 'columns'."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        # 1) Ghi cac dong rac/edge-case
        for row in MESSY_ROWS:
            writer.writerow(_row_for_schema(row, columns))

        # 2) Ghi khoi du lieu "sach" voi id duy nhat (co dich offset
        #    de tao TRUNG ID xuyen file mot cach co kiem soat o vung dau).
        for i in range(CLEAN_ROWS_PER_FILE):
            uid = id_offset + i
            cat = CATEGORIES[i % len(CATEGORIES)]
            price = round(10 + (i % 500) * 1.5, 2)  # gia hop le 10..~760
            row = {
                "id": uid,
                "product": f"Item-{uid}",
                "price": price,
                "category": cat,
                "currency": "USD",  # chi dung cho file co cot nay
            }
            writer.writerow(_row_for_schema(row, columns))


def generate_data_lake():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Moi file co SCHEMA khac nhau de test tinh "robust" cua pipeline:
    files = {
        # chuan
        "store_north.csv": ["id", "product", "price", "category"],
        # dao thu tu cot
        "store_south.csv": ["id", "category", "product", "price"],
        # thua cot 'currency'
        "store_east.csv":  ["id", "product", "price", "category", "currency"],
        # THIEU han cot 'category' -> pipeline phai xu ly graceful
        "store_west.csv":  ["id", "product", "price"],
    }

    # id_offset gan nhau (cach 50_000) trong khi moi file co 80_000 dong sach
    # => vung id [80_000..100_000) bi TRUNG xuyen file -> test dedup global.
    offsets = [10_000, 60_000, 110_000, 160_000]

    for (name, cols), offset in zip(files.items(), offsets):
        path = os.path.join(OUTPUT_DIR, name)
        _make_file(path, cols, offset)
        print(f"  - Created {path}  (schema={cols}, ~{CLEAN_ROWS_PER_FILE + len(MESSY_ROWS)} rows)")

    print(f"\nData lake created at ./{OUTPUT_DIR}/ with {len(files)} CSV files.")


if __name__ == "__main__":
    print("Generating poisoned data lake...")
    generate_data_lake()
