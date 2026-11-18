# Thiết Kế Kiến Trúc Hệ Thống Tổng Thể (High-Level & Low-Level System Design)

## 1. Sơ Đồ Kiến Trúc 6 Phân Vùng (`Zones A–F`), 17 Thành Phần (`[C1]`–`[C17]`), 10 Luồng Dữ Liệu (`1`–`10`)

```mermaid
flowchart LR
    subgraph ZA["Zone A: External Sources & Clients"]
        C1["[C1] GPS Fleet & Replay Producer\n(producer.py: 90,938 raw -> 90,787 accepted,\n~1% dup, ~5% late)"]
        C2["[C2] OSM / GIS Master Topology\n(577,967 raw -> 52,181 active nodes,\n84,206 clean motorized segments, 5,553 streets)"]
        C3["[C3] Traffic Ops & Commuter Clients\n(TOC, Sở GTVT, Signal API)"]
    end

    subgraph ZB["Zone B: Ingestion & Quality Funnel"]
        C4["[C4] Apache Kafka 4.1 KRaft\n(Topic: hcmc-traffic-events, 3 parts)"]
        C5["[C5] Ordered Quality Funnel (R1-R6)\n(try_cast, NFC 695->641, UTC+7,\nBurst Flag 7,192, Leak-Free Vmax)"]
    end

    subgraph ZC["Zone C: Speed Layer (Phase B: t > T_train)"]
        C6["[C6] Watermark (30m) & Dual-Sink Router\n(Sink 1: Bronze Append · Sink 2: Update)"]
        C7["[C7] O(1) Stream Sketch Engine\n(32KB RotatingBloom 0.0389%, 1KB HLL++, EWMA)"]
        C8["[C8] Online Broadcast Scorer\n(Read-Only .transform() -> STREAM_PROVISIONAL v1)"]
    end

    subgraph ZD["Zone D: Batch Layer (Phase A Fit & Phase C Recon)"]
        C9["[C9] Pure 5-Tuple Monoid,\n8-Bucket Key Salting & Spark 3.5.7 BHJ"]
        C10["[C10] MMDS Multi-Pass Mining Suite\n(PageRank No U-Turn 92,453 edges, Organic Rules, 7D K-Means)"]
        C11["[C11] Burst-Free Clean ML Trainer (Phase A)\n(1,885 organic TRAIN, class_weight^0.65, RF 23D)"]
        C12["[C12] Two-Tier Reconciliation Engine (Phase C)\n(Tier 1 exceptAll=0 · Tier 2 Backfill 500, Heal 609, Rescore 384/213)"]
    end

    subgraph ZE["Zone E: Zero-Conflict Frozen Registry"]
        C13A["[C13a] Frozen PipelineModel\n(outputs/model.joblib a2f411846508 + vmax ee7f692daad8)"]
        C13B["[C13b] Static Feature Store\n(84,206 clean segments, 74,211 Cold-Start, 75328ac2d5e3)"]
    end

    subgraph ZF["Zone F: Polyglot Storage & Unified Web TOC"]
        C14["[C14] Medallion Parquet Lakehouse\n(bronze_raw_events, stream_replay_windows, curated_gold/year_month=*)"]
        C15["[C15] Quarantine, WAL Checkpoints & Audit\n(quarantine_events.parquet 580, WAL 73.7 KB, mongo_verify.json)"]
        C16["[C16] MongoDB 8.0 Operational Store\n(bda501.traffic_scores 33,394 docs · Atomic $status_version + IXSCAN)"]
        C17["[C17] Unified Web TOC Command Center\n(ReactJS + FastAPI :8000 via toc-web)"]
    end

    C1 -->|"(1) Event-Time Stream"| C4
    C4 -->|"Kafka Events"| C5
    C2 -->|"(2) Static GIS Tables"| C5
    C5 -.->|"Quarantine (first_failing_rule)"| C15
    C5 -->|"(3) Validated Stream"| C6 --> C7 --> C8
    C5 -->|"(4) Validated Batch"| C9 --> C10 --> C11
    C6 -.->|"(5a) Sink 1: Bronze Raw 100%"| C14
    C7 -.->|"(5b) WAL Checkpoints (73.7 KB)"| C15
    C9 -->|"(6a) Gold Parquet (10m)"| C14
    C14 -->|"(6b) Backfill 500 late / Heal 609"| C12
    C11 -->|"(7a) Freeze SHA-256"| C13A
    C10 -->|"(7b) Freeze 84,206 segs"| C13B
    C13A & C13B -.->|"(8) Read-Only SHA-256"| C8 & C12
    C8 -->|"(9a) Upsert STREAM_PROVISIONAL (v1)"| C16
    C12 -->|"(9b) Atomic Guard Upsert (v2)"| C16
    C16 <-->|"(10a) IXSCAN (0.360 / 0.603 ms)"| C17 <-->|"(10b) Live Web :8000"| C3
```

---

## 2. Đặc Tả Toán Học Các Mô-Đun Cốt Lõi (`Low-Level Mathematical Specification`)

1. **Đại số `Pure 5-Tuple Commutative Monoid` (`[C9]`):**
   $$\mathbf{m}_{s,w} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt}), \quad \mathbf{e} = (0, 0.0, +\infty, -\infty, 0)$$
   $$\mathbf{m}_1 \oplus \mathbf{m}_2 = (\text{cnt}_1 + \text{cnt}_2, \textstyle\sum v_1 + \sum v_2, \min(\min v_1, \min v_2), \max(\max v_1, \max v_2), \text{sev}_1 + \text{sev}_2)$$
   Phép chia $\bar{v}_{s,w} = \sum v / \text{cnt}$ chỉ thực hiện duy nhất một lần ở bước cuối; khớp `100.0%` (`0` sai lệch trên `33,394` cửa sổ) với `baseline.py`.
2. **`Directed Line-Graph PageRank` Loại Bỏ `U-turn` & Bù `Dangling Nodes` (`[C10]`, `MMDS Ch. 5`):**
   Trên đồ thị chuyển tiếp đoạn đường cơ giới sạch $L(\mathcal{G}) = (\mathcal{E}, \mathcal{T})$ với $N = |\mathcal{E}| = 84{,}206$, $|\mathcal{T}| = 92{,}453$ (loại bỏ `dst_e_node == src_s_node` và `src_seg == dst_seg`) và $|\mathcal{D}| = 314$ đoạn cụt:
   $$r_s^{(k+1)} = \frac{1 - \beta}{N} + \beta \left( \sum_{u \in \text{In}(s)} \frac{r_u^{(k)}}{d_{\text{out}}(u)} + \frac{1}{N} \sum_{d \in \mathcal{D}} r_d^{(k)} \right), \quad \beta = 0.85$$
   Top `#1`: `21969 — Nguyễn Kiệm (street_level=2, PR=2.7799)`.
3. **Đặc trưng Nhân quả Mở rộng `Causal Expanding Empirical-Bayes Priors` ($\tau < t$, `[C11]`):**
   $$\text{Prior}_s(t) = \frac{\sum_{\tau < t} y_{s,\tau} + m_{\text{seg}} \cdot \bar{y}_{\text{global}}(\tau < t)}{\text{count}_s(\tau < t) + m_{\text{seg}}} \quad (m_{\text{seg}} = 5, \; m_{\text{street}} = 10)$$
   Chỉ tích lũy trên `TRAIN` hữu cơ (`is_synthetic_burst == 0`) với `max_abs_diff = 0.0` trước nhiễu tương lai.
4. **Kiểm chứng Kế hoạch Thực thi Vật lý `Spark 3.5.7` (`hcmc-traffic-spark:3.5.7`) & `MongoDB 8.0.34` (`[C9]`, `[C14]`, `[C16]`):**
   - `Catalyst Join Plan`: Giảm từ `5 Exchanges` (`SortMergeJoin`, `166.18 ms`) xuống `1 Exchange` (`BroadcastHashJoin` trên `segments` và `streets`, `123.37 ms`, **`25.76%` speedup**), `two_way_exceptAll_diff = 0`; `Monoid 97.92 ms` vs salted `185.78 ms`; Spark MLlib organic TRAIN (`FPGrowth = 32`, `KMeans = 3,609`, `RF = 1,885` đạt `test_acc = 0.9416, roc_auc = 0.9903`).
   - `Spark Structured Streaming`: Sink 2 ghi nhận watermark thực **`2020-12-01T12:30:00.000Z`** (`3` batches committed).
   - `MongoDB 8.0 Compound IXSCAN`: Ghi `33,394` bản ghi qua `4` phân vùng (`69` lô `500` bản ghi) với **atomic `$status_version` pipeline guard** (`25/25` stale `v1` rejected); truy vấn `UC-2` (`1,234` cửa sổ khớp) dùng `level_prob_segment_idx` (`in_memory_sort = false`, `nReturned = 25`, `totalKeysExamined = 26`, `totalDocsExamined = 25`), đạt **`warm_median = 0.360 ms / p95 = 0.603 ms`** so với **`COLLSCAN median = 11.289 ms` (`31.36x`)**.
