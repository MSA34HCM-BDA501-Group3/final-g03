# Nghiên Cứu Hành Vi Điều Hành & Luồng Tương Tác (Operator UX Research)

## 1. Luồng Ra Quyết Định Của Điều Phối Viên Giao Thông (`TOC Decision Flow`)

Qua phân tích 6 kịch bản nghiệp vụ (`UC-1` đến `UC-6`), người vận hành tại Trung tâm Quản lý Giao thông Đô thị TP.HCM (`[C3]`) không muốn đọc các bảng số liệu rời rạc mà cần một quy trình điều tra 5 bước liền mạch trên giao diện `[C17] Streamlit 5-Tab UI`:
1. **Bước 1 — Toàn cảnh & Chất lượng Dữ liệu (`Tab 1 — Overview, Quality Funnel & Hotspots`):** Kiểm tra ngay `Top-25 Decaying Hotspots` $C_s(t)$ (`UC-1`) để nhận diện điểm ùn tắc kéo dài, đồng thời xác nhận phễu lọc chất lượng `[C5]` và kho cách ly `[C15] Quarantine` (`UC-6`).
2. **Bước 2 — Dự báo Ngắn hạn & Truy vấn Tốc độ Cao (`Tab 2 — Live Forecast & IXSCAN Serving`):** Tra cứu dự báo `LOS (t+30m)` và xác suất kẹt nặng $\hat{p}_{\text{severe},t+1}$ cho từng cửa sổ hoặc truy vấn danh sách trục chính (`street_level IN [1,2]`, $\hat{p}_{\text{severe}} \ge 0.70$) kèm trạng thái vòng đời `STREAM_PROVISIONAL` vs. `BATCH_RECONCILED` (`UC-2`).
3. **Bước 3 — Chẩn đoán Nút cổ chai & Lan truyền Liên trục (`Tab 3 — Line-Graph PageRank & Co-Congestion Rules`):** Kiểm tra điểm nghẽn cấu trúc `Directed Line-Graph PageRank` trên toàn bộ `84,633` đoạn đường (bao gồm cả `74,606` đoạn `Cold-Start` vùng mù cảm biến — `UC-3`) và tra cứu `30` luật đồng kẹt xe `SON/FPGrowth` (`UC-4`).
4. **Bước 4 — Phân cụm Chế độ Hành vi & Kiểm định ML (`Tab 4 — 7D K-Means Regimes & Leak-Free Ablation`):** Khám phá `6` chế độ giao thông vĩ mô, tìm `Top-K` đoạn đường tương đồng (`UC-5`), và đối chứng bảng `Ablation Study` chứng minh hiệu quả của `Causal Expanding Priors` (`P3`).
5. **Bước 5 — Kiểm toán Luồng $O(1)$ & Đối soát Hai Tầng (`Tab 5 — Stream Sketches, Reconciliation & IXSCAN Evidence`):** Kiểm tra sai số `32KB Bloom Filter`, `1KB HyperLogLog++`, bảng đối soát `Two-Tier Reconciliation` (`98.50% -> 100.0%`), và bằng chứng kế hoạch thực thi `BroadcastHashJoin` & `MongoDB IXSCAN` (`UC-6`).
