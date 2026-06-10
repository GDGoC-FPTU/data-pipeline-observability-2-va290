# Experiment Report: Data Quality Impact on AI Agent

**Student ID:** AI20K-1008
**Name:** Đỗ VA
**Date:** 2026-06-10

---

## 1. Kết quả thí nghiệm

Chạy `agent_simulation.py` với 2 bộ dữ liệu và ghi lại kết quả:

| Scenario | Agent Response | Accuracy (1-10) | Notes |
|----------|----------------|-----------------|-------|
| Clean Data (`processed_data.csv`) | Agent: Based on my data, the best choice is Laptop at $1200. | 9 | Dữ liệu đã qua ETL (validate + transform). Agent chọn đúng sản phẩm electronics có giá hợp lý nhất. |
| Garbage Data (`garbage_data.csv`) | Agent: Based on my data, the best choice is Nuclear Reactor at $999999. | 2 | Agent bị đánh lừa bởi outlier $999999. Câu trả lời vô nghĩa về mặt thực tế. |

---

## 2. Phân tích & Nhận xét (Analysis)

### Tại sao Agent trả lời sai khi dùng Garbage Data?

Agent trả lời sai vì dữ liệu đầu vào chưa qua bước validate và làm sạch. File `garbage_data.csv`
chứa nhiều lỗi chất lượng dữ liệu điển hình. Thứ nhất là **outlier** (giá trị ngoại lai): bản ghi
"Nuclear Reactor" có giá 999999 không phải giá thực tế, nhưng vì Agent dùng logic `idxmax()` để chọn
sản phẩm đắt nhất nên nó bị kéo theo outlier này và đưa ra kết luận sai. Thứ hai là **wrong data
type** (sai kiểu dữ liệu): cột price chứa chuỗi "ten dollars" khiến kiểu dữ liệu của cả cột bị pandas
suy luận thành object, dễ gây lỗi tính toán hoặc so sánh. Thứ ba là **duplicate IDs**: id=1 xuất hiện
hai lần (Laptop và Banana) gây nhập nhằng khi tra cứu theo khóa. Thứ tư là **null values**: bản ghi
"Ghost Item" có id và category đều None, cùng với price=0, là rác thực sự và lẽ ra phải bị loại bỏ.
Tất cả những vấn đề này không được làm sạch trước khi nạp vào Agent, nên Agent "rác vào thì rác ra"
(garbage in, garbage out) và cho kết quả sai lệch hoàn toàn so với dữ liệu sạch.

---

## 3. Kết luận

**Quality Data > Quality Prompt?** Đồng ý.

Dù Agent có logic (prompt) tốt đến đâu, nếu dữ liệu đầu vào là rác thì kết quả vẫn sai. Trong thí
nghiệm này cùng một câu hỏi và cùng một Agent, nhưng chỉ cần đổi bộ dữ liệu, kết quả từ đúng
(Laptop) thành sai (Nuclear Reactor). Bước ETL validate/transform giúp loại outlier, chuẩn hóa kiểu
dữ liệu và loại null là nền tảng quyết định độ tin cậy của hệ thống AI. Vì vậy, đầu tư vào chất lượng
dữ liệu quan trọng hơn việc chỉ tối ưu prompt.
