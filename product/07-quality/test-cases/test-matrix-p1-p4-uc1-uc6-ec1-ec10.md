# Ma Trận Kiểm Thử Toàn Diện (Comprehensive Test Matrix: P1–P4, UC-1–UC-6, EC-1–EC-10)

| Mã Test Case | Đối Tượng Kiểm Thử (`Target`) | Điều Kiện Kiểm Chứng (`Assertion`) | Trạng Thái |
| :--- | :--- | :--- | :---: |
| **`TC-P1-01`** | `P1` & `EC-1`: `3-Tier v_max Imputation` | `dataset["max_velocity"].isna().sum() == 0` & phân bổ đủ `TIER_1_SEGMENT_OBSERVED (4,946)`, `TIER_1_SEGMENT_TRAIN_P85 (8,164)`, `TIER_2 (20,124)`, `TIER_3 (207)` trên `33,441` cửa sổ và `84,633` đoạn đường | **PASS** |
| **`TC-P1-02`** | `P1` & `EC-2`: `Ordered Quality Funnel & Quarantine` | `154` đoạn `length <= 0m` được ghi nhận vào `quarantine_summary.csv` và nội suy thành `27.0m`; `length_m > 0` đạt `100%` | **PASS** |
| **`TC-P1-03`** | `P1` & `EC-8`: `Commutative Monoid vs. Baseline` | `monoid_baseline_verification.json` xác nhận `discrepancy_count == 0` trên toàn bộ `33,441` cửa sổ | **PASS** |
| **`TC-P2-01`** | `P2`, `UC-3` & `EC-7`: `Orphan Pruning & Line-Graph PageRank` | `active_nodes == 52,425`, `pruned_orphan_nodes == 525,542`, `dangling_nodes == 48`, $\sum \text{PR} = 84{,}633.0$, Top-1 là `Seg 61781` (`PR = 3.7515`) | **PASS** |
| **`TC-P2-02`** | `P2`, `UC-4` & `EC-3`: `NFC Street Hygiene & 2-Pass SON` | `30` luật có hướng (`A => B` và `B => A`) trong `co_congestion_rules.csv`, `0` luật tự suy diễn (`antecedent != consequent`), Top-1 `Lý Chính Thắng => Nguyễn Thông` (`Lift = 28.5833`) | **PASS** |
| **`TC-P2-03`** | `P2`, `UC-5` & `EC-5`: `Train-Only 7D K-Means & Cold-Start` | `clusters_populated == 6` (`6,926` đoạn `TRAIN`), `Silhouette == 0.2853` (`DBI = 1.1181`), `segment_static_features.parquet` bao phủ đủ `84,633` đoạn (`74,606` `Cold-Start`) | **PASS** |
| **`TC-P3-01`** | `P3`, `EC-4`, `EC-5`, `EC-6`: `Leak-Free Causal ML & Ablation (+30m)` | `RF (+Graph, Priors & K-Means, 23D)` trên `7,898` cặp `t+30m` đạt `Severe F1 = 0.9293`, `PR-AUC = 0.9621`, `ROC-AUC = 0.9901` (legacy `shift(-1)`: `F1 = 0.6447`, `PR-AUC = 0.7628`); `[C13a/b]` bất biến | **PASS** |
| **`TC-P4-01`** | `P4`, `UC-1` & `EC-8`: `Bloom 32KB, HLL++ 1KB & Decaying Hotspots` | `Bloom` (`uint8[32768]`) bắt `334/334` bản ghi trùng (`FPR = 0.00616`); `HLL++` (`uint8[1024]`) sai số `3.98%` (`9,627.9`); `hotspot_alerts.csv` xuất đủ `Top-25` | **PASS** |
| **`TC-P4-02`** | `P4`, `UC-6` & `EC-9`: `Two-Tier Reconciliation & Lag Healing` | `in_watermark_like_for_like_match_pct == 100.0`, `501` sự kiện trễ trên `219` khung `(date, period)`, hàn gắn `3,953` cửa sổ trễ bậc xuôi dòng, `post_reconciliation_completeness_pct == 100.0` | **PASS** |
| **`TC-P4-03`** | `P4`, `UC-2` & `EC-10`: `MongoDB 8.0 Upsert & IXSCAN` | `mongo_verify.json` xác nhận `verified_against_mongo_8_0 == True`, `winning_stages` chứa `IXSCAN`, `in_memory_sort == False`, `warm_p95_ms == 0.767 ms` | **PASS** |
| **`TC-Q-SPARK`** | `Pillars I1 & I2` (`PR #1`): `spark_batch.py` & `traffic_data/spark_etl.py` | `test_quyen_spark.py` (`5/5 OK` trên `Spark 3.5.7` với `ansi.enabled=true`): `3` `BroadcastHashJoin` / `0` `SortMergeJoin`, 1-stage vs 2-stage salted Monoid `exceptAll == 0`, `date_utc` + `date` ICT, `write_task_metrics` rolled event logs | **PASS** |

