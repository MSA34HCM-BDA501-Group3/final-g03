# Lộ Trình Mở Rộng Quy Mô & Vận Hành Bền Vững (Growth & Scaling Roadmap — Phase 11 & Gate 5)

## 1. Lộ Trình Mở Rộng Kỹ Thuật Toàn Thành Phố (`City-Wide Scaling Roadmap`)

Khi mở rộng độ phủ cảm biến GPS từ `10,027` đoạn đường hiện tại (`11.85%`) lên toàn bộ `84,633` đoạn đường (`100%`) của TP.HCM ở tần suất cập nhật 30 phút (`4.06 triệu cửa sổ/ngày` $\approx$ `1.48 tỷ bản ghi/năm`), kiến trúc V3 mở rộng mượt mà theo trục ngang (`Horizontal Scale-Out`) mà không cần thay đổi logic thuật toán:

| Tầng Kiến Trúc | Cấu Hình Hiện Tại (`16 GB Single-Host`) | Cấu Hình Mở Rộng Toàn Thành Phố (`Production Cluster`) |
| :--- | :--- | :--- |
| **`Zone B` (`[C4] Kafka KRaft`)** | `1` Broker KRaft · `3` Partitions (`1.5 GB RAM`) | Cụm `3` Brokers KRaft · `32` Partitions băm theo `segment_id` (`Replication Factor = 3`). |
| **`Zone C` (`[C6]–[C8] Speed Layer`)** | `32 KB Bloom Filter` + `1 KB HLL++` + `Decaying Map` | Giữ nguyên cấu trúc `O(1) Sketches` phân tán theo phân vùng; tăng `Bloom Filter` lên `2 MB` ($2^{24}$ bits) cho lưu lượng hàng triệu sự kiện/giờ. |
| **`Zone D` (`[C9]–[C12] Batch & ML`)** | `local[4]` (`6.0 GB RAM`) · `2-Stage Key Salting (0..7)` | Cụm `Spark Standalone / Kubernetes` (`4` Executors $\times$ `8 GB RAM`); mở rộng `Key Salting` lên `0..31` cho khung giờ cao điểm. |
| **`Zone F` (`[C14] Lakehouse & [C16] Mongo`)** | `Partitioned Parquet (year_month)` + `MongoDB 8.0` đơn lẻ | `Apache Iceberg / Delta Lake` trên `MinIO / S3` + `MongoDB 8.0 Replica Set / Sharded Cluster` theo `segment_id`. |

---

## 2. Đánh Giá Cổng Kiểm Soát `Gate 5 — Scale Readiness`

- [x] **1. Khả năng Mở rộng Tuyến tính (`Linear Scalability`):** Mọi phép gộp dữ liệu đều được biểu diễn dưới dạng `Commutative Monoid` kết hợp `2-Stage Key Salting`, và mọi phép đếm luồng đều dùng cấu trúc `O(1) Sketching` (`Bloom Filter`, `HyperLogLog++`, `Exponentially Decaying Window`).
- [x] **2. Năng lực Tự Phục hồi & Kiểm toán (`Self-Healing & Auditability`):** Cơ chế `Two-Tier Reconciliation`, `WAL Checkpoints`, `Quarantine Store` và `Partitioned Idempotent Upsert` đảm bảo hệ thống tự chữa lành khi mạng trễ hoặc container khởi động lại.
- [x] **3. Hoàn thiện Quản trị Sản phẩm (`Full Lifecycle Governance`):** Hoàn tất đầy đủ 11 giai đoạn, 5 `Stage-Gates` và 8 `Workstreams` theo chuẩn `product/`.
- **Kết luận `Gate 5`:** **PASSED (Hệ thống hoàn tất toàn diện vòng đời sản phẩm V3).**
