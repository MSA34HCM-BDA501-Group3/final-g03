# Chiến Lược Triển Khai & Go-To-Market (Wave 2 Platform)

## 1. Giá trị Cốt lõi (Value Proposition) cho Sở Giao thông Vận tải & Trung tâm Điều hành (TOC)
1. **Giám sát Toàn mạng Lưới (`84,206` Đoạn Đường Cơ Giới Sạch):** Kết hợp trực tiếp `9,995` đoạn đường có GPS (`11.87%`) với chỉ số cấu trúc PageRank (`#1: 21969 Nguyễn Kiệm, PR = 2.7799`) và không gian hành vi `K-Means 7D` để lập chỉ mục cho toàn bộ `74,211` đoạn đường Cold-Start (`88.13%`), đồng thời công khai trung thực giới hạn vật lý của proxy không gian (`Masked-GPS Cold-Start Proxy Acc = 0.5271, ROC-AUC = 0.6175`).
2. **Dự báo Đáng tin Cậy & Sạch Rò rỉ (`RF 23D`):** Loại bỏ hoàn toàn `3,596` cặp synthetic burst khỏi huấn luyện, không dùng `sample_weight` nhân tạo; đạt `TEST ROC-AUC = 0.9900`, `PR-AUC = 0.9595`, `Severe F1 = 0.9301`, `Onset risk lift = 3.32x` và bắt chuyển pha tắc nghẽn vượt trội Persistence (`McNemar p = 0.015625`, `Detect ROC-AUC = 0.9146` vs heuristic `0.7921`).
3. **Khai phá Lan truyền Tắc nghẽn Thực chứng:** Cung cấp `12` luật đồng tắc nghẽn (`Nguyễn Thị Minh Khai <-> Cống Quỳnh`, Lift `15.2308`) và `14` luật lan truyền có kiểm soát tính tự duy trì (`persistence_controlled = True`, tiêu biểu `Lê Quang Định -> Trần Quang Khải`, Lift `24.0`).
4. **Hiệu năng Truy vấn & Giao diện Điều hành Kép:**
   - **Web TOC Command Center (`FastAPI + React/Vite :8000`):** Bản đồ điều hành trực quan và kịch bản câu chuyện vận hành theo thời gian thực.
   - **Streamlit Operations Console (`:8501`):** Phân tích chi tiết 5 tab cho kỹ sư dữ liệu & kiểm toán viên.
   - **MongoDB 8.0.34 Sub-millisecond Serving:** Độ trễ `IXSCAN` ấm đạt `0.389 ms` median / `0.692 ms` p95 (`23.55x` so với `COLLSCAN`) cho `33,394` tài liệu.

## 2. Lộ trình Triển khai Thí điểm (Phased Adoption)
- **Giai đoạn 1 — Shadow Operation:** Đấu nối nguồn GPS trực tiếp qua Kafka KRaft + `live-consumer`, kiểm tra `tier1_exceptAll_diff = 0` và `atomic $status_version` guard.
- **Giai đoạn 2 — Corridor Dispatch:** Kích hoạt cảnh báo sớm theo `1,202` cảnh báo trục chính và `14` luật lan truyền trễ trên `Web TOC (:8000)` cho đội điều tiết giao thông.
- **Giai đoạn 3 — Full City Lakehouse Integration:** Nối mở rộng nguồn camera/cảm biến vòng từ bổ sung vào lớp `curated_gold/year_month=*` để thu hẹp tỷ lệ `88.13%` đoạn đường Cold-Start.
