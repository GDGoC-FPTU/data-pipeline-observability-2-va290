[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=24112772&assignment_repo_type=AssignmentRepo)
# Day 10 Lab: Data Pipeline & Data Observability

**Student Email:** 26ai.anhddv@vinuni.edu.vn
**Student ID:** AI20K-1008
**Name:** Đỗ VA

---

## Mô tả

Bài lab xây dựng một **ETL Pipeline** hoàn chỉnh và minh họa tầm quan trọng của
**Data Observability** đối với hệ thống AI Agent. Pipeline đọc dữ liệu thô từ `raw_data.json`,
validate để loại bỏ các bản ghi lỗi (giá <= 0, category rỗng), transform (giảm giá 10%, chuẩn hóa
category về Title Case, thêm timestamp `processed_at`), rồi lưu kết quả sạch ra `processed_data.csv`.
Sau đó tôi chạy **stress test** so sánh hành vi của Agent trên dữ liệu sạch và dữ liệu rác để chứng
minh nguyên tắc "garbage in, garbage out".

---

## Cách chạy (How to Run)

### Prerequisites
```bash
pip install pandas pytest
```

### Chạy ETL Pipeline
```bash
python solution.py
```
Kết quả: tạo file `processed_data.csv` (3 records kept, 2 dropped).

### Chạy Agent Simulation (Stress Test)
```bash
# Bước 1: tạo dữ liệu rác
python generate_garbage.py

# Bước 2: chạy Agent trên cả dữ liệu sạch và dữ liệu rác
python agent_simulation.py
```

### Chạy Autograder Tests (local)
```bash
python -m pytest tests/test_autograder.py -v
```

---

## Cấu trúc thư mục

```
├── solution.py              # ETL Pipeline script (extract/validate/transform/load)
├── raw_data.json            # Dữ liệu gốc
├── generate_garbage.py      # Tạo garbage_data.csv (dữ liệu rác)
├── agent_simulation.py      # Mô phỏng Agent (stress test Clean vs Garbage)
├── processed_data.csv       # Output của pipeline
├── experiment_report.md     # Báo cáo thí nghiệm
└── README.md                # File này
```

---

## Kết quả

- **Validation:** 5 records đầu vào -> giữ lại **3**, loại **2** (id=3 giá <= 0, id=4 category rỗng).
- **Transform:** thêm cột `discounted_price` (price * 0.9), chuẩn hóa `category` (Title Case), thêm
  `processed_at`.
- **Stress Test:**
  - Clean Data -> Agent trả lời đúng: *"the best choice is Laptop at $1200"*.
  - Garbage Data -> Agent trả lời sai do outlier: *"the best choice is Nuclear Reactor at $999999"*.
- **Kết luận:** Quality Data > Quality Prompt — chất lượng dữ liệu quyết định độ tin cậy của Agent.

---

## Bonus: Parallel Multi-CSV Pipeline (Nâng cao)

Phần mở rộng nâng pipeline từ xử lý **1 file JSON** lên xử lý **song song nhiều file CSV** trong một
thư mục (`data_lake/`), với kịch bản dữ liệu khó hơn nhiều.

### Cách chạy

```bash
# 1. Sinh "data lake" gồm nhiều file CSV độc (poisoned)
python generate_data_lake.py

# 2. Chạy pipeline song song + benchmark 3 chế độ
python pipeline_parallel.py
```

Kết quả: tạo `processed_lake.csv` (dữ liệu sạch đã gộp). *Lưu ý: `data_lake/` và `processed_lake.csv`
được `.gitignore` vì tái tạo được bằng lệnh trên.*

### Kịch bản khó hơn (`generate_data_lake.py`)

- **Schema khác nhau** giữa các file: đảo thứ tự cột, thừa cột `currency`, và một file **thiếu hẳn
  cột `category`**.
- **Giá bẩn đủ kiểu:** `"$1,200.50"`, `"1.2e3"`, `"ten dollars"`, `"N/A"`, chuỗi rỗng, số âm, số 0,
  có khoảng trắng thừa.
- **Trùng ID** trong cùng file và **xuyên nhiều file** (vùng ID chồng lấn có kiểm soát).
- **Outlier cực đoan** (`999999`), null values, category hoa/thường/whitespace.

### Pipeline xử lý (`pipeline_parallel.py`)

| Tính năng | Mô tả |
|-----------|-------|
| **Multi-file** | Quét toàn bộ `*.csv` trong folder bằng `glob`. |
| **Parallelism** | So sánh 3 chế độ: Sequential vs `ThreadPoolExecutor` vs `ProcessPoolExecutor` (đo wall-clock). |
| **Robust ETL** | Ép kiểu giá (bỏ `$` `,`, parse khoa học), chịu schema lệch & file thiếu cột. |
| **Global dedup** | Loại trùng ID xuyên file (`drop_duplicates`). |
| **IQR outlier rule** | Loại giá bất thường bằng Tukey's fences `[Q1-1.5·IQR, Q3+1.5·IQR]` trên toàn bộ phân phối (bắt được `999999`). |
| **Observability** | Log per-file (read/kept/dropped + lý do), thêm cột `source_file` + `processed_at`. |

### Kết quả benchmark (4 file, ~320k dòng, 8 cores)

| Mode | Wall-clock | Speedup |
|------|-----------|---------|
| Sequential | ~1.0s | 1.00x |
| ThreadPool | ~1.0s | ~1.0x (bị **GIL** chặn vì transform là CPU-bound) |
| ProcessPool | ~0.7–0.9s | ~1.2–1.4x (đa tiến trình thật, nhưng có overhead pickle) |

**Bài học:** với tác vụ CPU-bound, đa luồng (thread) gần như vô ích do GIL của Python; phải dùng đa
tiến trình (process) để tận dụng nhiều core. ProcessPool không đạt 4x lý tưởng vì chi phí khởi tạo
process và truyền dữ liệu (pickle) giữa các tiến trình.
