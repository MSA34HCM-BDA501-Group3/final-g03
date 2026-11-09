# Thiết Kế Kiến Trúc Hệ Thống Tổng Thể (High-Level & Low-Level System Design — V3)

## 1. Sơ Đồ Kiến Trúc 6 Phân Vùng (`Zones A–F`), 17 Thành Phần (`[C1]`–`[C17]`), 10 Luồng Dữ Liệu (`1`–`10`)

```mermaid
flowchart LR
    subgraph ZA["Zone A: External Sources & Clients"]
        C1["[C1] GPS Fleet & Replay Producer\n(producer.py: 90,938 pings,\n1% dup, 5% late)"]
        C2["[C2] OSM / GIS Master Topology\n(577,967 raw -> 52,425 active nodes,\n84,633 segments, 5,553 streets)"]
        C3["[C3] Traffic Ops & Commuter Clients\n(TOC, Sở GTVT, Signal API)"]
    end

    subgraph ZB["Zone B: Ingestion & Quality Funnel"]
        C4["[C4] Apache Kafka 4.1 KRaft\n(Topic: hcmc-traffic-events, 3 parts)"]
        C5["[C5] Ordered Quality Funnel (Pillar I2)\n(try_cast, NFC Hygiene, UTC+7,\n3-Tier v_max Imputation)"]
    end

    subgraph ZC["Zone C: Speed Layer (Phase B: t > T_train)"]
        C6["[C6] Watermark (30m) & Dual-Sink Router\n(Sink 1: Bronze Append · Sink 2: Update)"]
        C7["[C7] O(1) Stream Sketch Engine (MMDS Ch.4)\n(32KB Bloom, 1KB HLL++, Decaying C_s(t))"]
        C8["[C8] Online Broadcast Scorer\n(Read-Only .transform() -> STREAM_PROVISIONAL)"]
    end

    subgraph ZD["Zone D: Batch Layer (Phase A Fit & Phase C Recon)"]
        C9["[C9] Commutative Monoid Combiner,\n2-Stage Key Salting & BroadcastHashJoin"]
        C10["[C10] MMDS Multi-Pass Mining Suite\n(Ch.5 Line-Graph PR, Ch.6 FPGrowth, Ch.7 7D K-Means)"]
        C11["[C11] Leak-Free Causal ML Trainer (Phase A)\n(Causal Priors <= t, 70/15/15 Split, .fit())"]
        C12["[C12] Two-Tier Reconciliation Engine (Phase C)\n(Tier 1 exceptAll==0 · Tier 2 Backfill 501 seg-windows / 219 slots)"]
    end

    subgraph ZE["Zone E: Zero-Conflict Frozen Registry"]
        C13A["[C13a] Frozen PipelineModel\n(outputs/model.joblib + Spark PipelineModel)"]
        C13B["[C13b] Static Feature Store\n(84,633 segments + Cold-Start)"]
    end

    subgraph ZF["Zone F: Polyglot Storage & Serving UI"]
        C14["[C14] Medallion Parquet Lakehouse\n(bronze_raw, silver, curated_gold/year_month=*)"]
        C15["[C15] Quarantine, WAL Checkpoints & Audit\n(quarantine/, checkpoints/, mongo_verify.json)"]
        C16["[C16] MongoDB 8.0 Operational Store\n(bda501.traffic_scores · Compound IXSCAN)"]
        C17["[C17] Streamlit 5-Tab Operational UI\n(:8501 Gateway)"]
    end

    C1 -->|"(1) Event-Time Stream"| C4
    C2 -->|"(2) Static GIS Tables"| C5
    C4 -->|"(3a) Online Feed"| C6
    C4 -.->|"(3b) Raw GPS Feed"| C5
    C5 -->|"(4a) Clean Windows & Dims"| C9
    C5 -.->|"(4b) First-Failing Rule"| C15
    C6 -->|"(5) In-WM Windows"| C7 --> C8
    C6 -->|"(6a) Sink 1 Raw + WAL"| C14 & C15
    C9 -->|"(7) Phase A Batch Flow"| C10 --> C11
    C9 & C10 -->|"(6b) Gold Parquet"| C14
    C14 -->|"(6c) 100% Raw Events"| C12
    C11 -->|"(7) Freeze Artifacts"| C13A & C13B
    C13A & C13B -.->|"(8) Read-Only .transform()"| C8 & C12
    C8 -->|"(9a) Upsert STREAM_PROVISIONAL (v1)"| C16
    C12 -->|"(9b) Upsert BATCH_RECONCILED (v2)"| C16
    C14 & C15 & C16 -->|"(10) IXSCAN & Lakehouse"| C17 --> C3
```

---

## 2. Đặc Tả Toán Học Các Mô-Đun Cốt Lõi (`Low-Level Mathematical Specification`)

1. **Vị nhóm Giao hoán `Commutative Monoid` (`[C9]`, `Pillar I1`):**
   $$\mathbf{m}_{s,w} = (\text{cnt}, \sum v, \min v, \max v, \text{severe\_cnt}), \quad \mathbf{e} = (0, 0.0, +\infty, 0.0, 0)$$
   $$\mathbf{m}_1 \oplus \mathbf{m}_2 = (\text{cnt}_1 + \text{cnt}_2, \textstyle\sum v_1 + \sum v_2, \min(\min v_1, \min v_2), \max(\max v_1, \max v_2), \text{sev}_1 + \text{sev}_2)$$
   Phép chia $\bar{v}_{s,w} = \sum v / \text{cnt}$ và suy diễn nhãn $\text{LOS}_{s,w}$ chỉ được thực hiện duy nhất một lần tại bước `final_aggregate`.
2. **`Directed Line-Graph PageRank` Có Bù Nút Cụt (`[C10]`, `MMDS Ch. 5`):**
   Trên đồ thị chuyển tiếp đoạn đường $L(\mathcal{G}) = (\mathcal{E}, \mathcal{T})$ với $N = |\mathcal{E}| = 84{,}633$, $|\mathcal{T}| = 151{,}929$ và tập nút cụt $\mathcal{D} = \{s \in \mathcal{E} \mid d_{\text{out}}(s) = 0\}$ ($|\mathcal{D}| = 48$):
   $$r_s^{(k+1)} = \frac{1 - \beta}{N} + \beta \left( \sum_{u \in \text{In}(s)} \frac{r_u^{(k)}}{d_{\text{out}}(u)} + \frac{1}{N} \sum_{d \in \mathcal{D}} r_d^{(k)} \right), \quad \beta = 0.85$$
3. **Đặc trưng Nhân quả Mở rộng `Causal Expanding Empirical-Bayes Priors` (`[C11]`, `Pillar I3`):**
   $$\text{Prior}_s(t) = \frac{\sum_{\tau < t} y_{s,\tau} + m_{\text{seg}} \cdot \bar{y}_{\text{global}}(\tau \le t)}{\text{count}_s(\tau < t) + m_{\text{seg}}} \quad (m_{\text{seg}} = 5, \; m_{\text{street}} = 10)$$
   Mọi quan sát có cùng `slot_ts` trên cùng tuyến đường được gom theo nhóm timestamp trước khi tính tổng tích lũy mở rộng (`expanding sum`), đảm bảo bất biến nhân quả tuyệt đối trước mọi nhiễu tương lai tại $\tau > t$.
4. **Kiểm chứng Kế hoạch Thực thi Vật lý `Spark 3.5.7` & `MongoDB 8.0.34` (`[C9]`, `[C14]`, `[C16]`):**
   - `Catalyst Join Plan`: Giảm từ `10 Exchanges` (`SortMergeJoin`) xuống `2 Exchanges` (`BroadcastHashJoin` trên bảng chiều `segments` và `streets`), với `exceptAll() == 0` hai chiều giữa `DataFrame DSL` và `Spark SQL`.
   - `MongoDB 8.0 Compound IXSCAN`: Ghi `33,441` bản ghi qua `4` phân vùng (`69` lô `500` bản ghi `UpdateOne(upsert=True)`), truy vấn `UC-2` sử dụng chỉ mục phức hợp `level_prob_segment_idx` (`{street_level: 1, severe_prob: -1, segment_id: 1}`) với `in_memory_sort = false`, `nReturned = 25`, `totalDocsExamined = 25`, và độ trễ `warm_p95_ms = 0.767 ms < 10 ms`.

