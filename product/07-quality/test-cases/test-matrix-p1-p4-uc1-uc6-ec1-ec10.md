# Ma Trận Kiểm Thử Toàn Diện (Comprehensive Test Matrix: P1–P4, UC-1–UC-6, EC-1–EC-10)

| Mã Test Case | Đối Tượng Kiểm Thử (`Target`) | Điều Kiện Kiểm Chứng (`Assertion`) | Trạng Thái |
| :--- | :--- | :--- | :---: |
| **`TC-P1-01`** | `P1` & `EC-1`: `3-Tier v_max Imputation` | `dataset["max_velocity"].isna().sum() == 0` & phân bổ đủ 3 tầng nội suy trên `33,441` cửa sổ và `84,633` đoạn đường | **PASS** |
| **`TC-P1-02`** | `P1` & `EC-2`: `Ordered Quality Funnel & Quarantine` | `154` đoạn `length <= 0m` được ghi nhận vào `quarantine_summary.csv` và nội suy thành `27.0m`; `length_m > 0` đạt `100%` | **PASS** |
| **`TC-P1-03`** | `P1` & `EC-8`: `Commutative Monoid vs. Baseline` | `monoid_baseline_verification.json` xác nhận `discrepancy_count == 0` trên toàn bộ `33,441` cửa sổ | **PASS** |
| **`TC-P2-01`** | `P2`, `UC-3` & `EC-7`: `Orphan Pruning & Line-Graph PageRank` | `active_nodes == 52,425`, `pruned_orphan_nodes == 525,542`, `dangling_nodes == 48`, $\sum \text{PR} = 84{,}633.0$, Top-1 là `Seg 61781` (`PR = 3.7515`) | **PASS** |
| **`TC-P2-02`** | `P2`, `UC-4` & `EC-3`: `NFC Street Hygiene & FPGrowth` | `30` luật trong `co_congestion_rules.csv`, `0` luật tự suy diễn (`antecedent != consequent`), Top-1 `Lý Chính Thắng => Nguyễn Thông` (`Lift = 28.5833`) | **PASS** |
| **`TC-P2-03`** | `P2`, `UC-5` & `EC-5`: `Train-Only 7D K-Means & Cold-Start` | `clusters_populated == 6`, `Silhouette == 0.3158`, `segment_static_features.parquet` bao phủ đủ `84,633` đoạn (`74,606` `Cold-Start`) | **PASS** |
| **`TC-P3-01`** | `P3`, `EC-4`, `EC-5`, `EC-6`: `Leak-Free Causal ML & Ablation` | `RF (+Graph, Priors & K-Means)` đạt `Severe F1 = 0.6670`, `PR-AUC = 0.7568`, `ROC-AUC = 0.8458`; `[C13a]` đóng băng bất biến | **PASS** |
| **`TC-P4-01`** | `P4`, `UC-1` & `EC-8`: `Bloom, HLL++ & Decaying Hotspots` | `Bloom` bắt `334/334` bản ghi trùng (`FPR = 0.006908`); `HLL++` sai số `3.98%`; `hotspot_alerts.csv` xuất đủ `Top-25` | **PASS** |
| **`TC-P4-02`** | `P4`, `UC-6` & `EC-9`: `Two-Tier Reconciliation` | `in_watermark_like_for_like_match_pct == 100.0`, `affected_late_windows == 319`, `post_reconciliation_completeness_pct == 100.0` | **PASS** |
| **`TC-P4-03`** | `P4`, `UC-2` & `EC-10`: `MongoDB 8.0 Upsert & IXSCAN` | `mongo_verify.json` xác nhận `indexes_valid == True`, `winning_stages` chứa `IXSCAN`, `in_memory_sort == False` | **PASS** |
