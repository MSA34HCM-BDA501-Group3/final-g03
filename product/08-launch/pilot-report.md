# Báo Cáo Vận Hành Thử Nghiệm (Pilot & Beta Evaluation Report — Phase 08 & Gate 4)

## 1. Phạm Vi Đợt Chạy Thử Nghiệm (`Pilot Scope`)

Hệ thống đã được chạy kiểm thử thực nghiệm toàn trình (`End-to-End Pilot Run`) trên toàn bộ lịch sử 10 tháng của bộ dữ liệu giao thông TP.HCM (`2020-07-03` đến `2021-04-22`) gồm `577,967` nút tọa độ, `84,633` đoạn đường, `90,938` bản tin vận tốc GPS và `33,441` cửa sổ 30 phút.

## 2. Kết Quả Đo Lường Thực Nghiệm (`Empirical Pilot Findings`)

1. **Hiệu quả Khai phá Điểm nghẽn Vùng mù Cảm biến (`UC-3`):** Thuật toán `Directed Line-Graph PageRank` đã xếp hạng thành công toàn bộ `84,633` đoạn đường trong `< 1.5 giây`, phát hiện chính xác các nút cổ chai đầu cầu và giao lộ trọng điểm như `Segment 61781` (Bến Phú Định, `PR = 3.7515`), `Segment 50017` (Lê Lai, `PR = 3.4650`), `Segment 62682` (Châu Văn Liêm, `PR = 2.9246`), và `Segment 21969` (Nguyễn Kiệm, `PR = 2.7313`).
2. **Hiệu quả Phát hiện Lan truyền Tắc nghẽn (`UC-4`):** Sau khi áp dụng `Unicode NFC Street Hygiene`, thuật toán `SON / FPGrowth` đã loại bỏ `100%` luật tự suy diễn giả tạo và tìm ra `30` luật lan truyền thực tế có ý nghĩa điều hành cao (ví dụ: `Lý Chính Thắng => Nguyễn Thông` với `Confidence = 100%`, `Lift = 28.58x`; `Cộng Hòa => Trường Chinh` với `Confidence = 88.89%`, `Lift = 12.20x`).
3. **Độ chính xác Dự báo & Chống Rò rỉ (`UC-2`, `UC-5`):** Trên `3,513` cửa sổ kiểm thử tương lai (`1,177` cửa sổ kẹt nặng `LOS E/F`), mô hình `Random Forest (+Graph PageRank, Causal Priors & K-Means)` đạt **`Severe F1 = 0.6670`** và **`PR-AUC = 0.7568`**.

## 3. Đánh Giá Cổng Kiểm Soát `Gate 4 — Product-Market Fit Signals`

- [x] **Tín hiệu Giá trị Thực tế:** Giải quyết trọn vẹn cả 6 kịch bản nghiệp vụ (`UC-1` đến `UC-6`) với số liệu định lượng rõ ràng và giao diện điều hành `Streamlit 5-Tab UI` phản hồi tức thời.
- **Kết luận `Gate 4`:** **PASSED.**
