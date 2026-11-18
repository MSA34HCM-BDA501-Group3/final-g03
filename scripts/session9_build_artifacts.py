"""G7 authoring with the repository's Python toolchain, explicitly authorized by user.

Builds the manual, seven-chapter report and 16-slide deck from the SAME evidence.
Prior binaries and editable narratives are archived once, never silently discarded.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image as PILImage

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = ROOT / "evidence/session9"
BUILD = ROOT / "tmp/g7"
FIGURES = BUILD / "figures"
TEAM = "Nguyễn Tạ Minh Quyền (25MS23280) · Nguyễn Thành Đạt (25MS23306) · Phạm Công Minh (25MS23310)"
INSTRUCTOR = "Đặng Trần Khánh"
NAVY, SKY, MUTED = "#0F172A", "#0284C7", "#475569"


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def archive():
    directory = EVIDENCE / "g7_archive"
    directory.mkdir(exist_ok=True)
    files = [
        "BDA501_Session9_Lab_Manual_G03.docx",
        "HCMC_Smart_Traffic_Final_Report.pdf",
        "HCMC_Smart_Traffic_Final_Presentation.pptx",
        "HCMC_Smart_Traffic_Final_Slides.pdf",
        "PRESENTATION_SCRIPT.md",
        "README.md",
        "TECHNICAL_NOTE.md",
        "product/00-proposal/project-proposal.md",
    ]
    hashes = {}
    for name in files:
        target = directory / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copy2(ROOT / name, target)
        hashes[name] = hashlib.sha256(target.read_bytes()).hexdigest()
    (directory / "archive_hashes.json").write_text(json.dumps(hashes, indent=2), encoding="utf-8")


def evidence():
    import joblib

    data = {
        name: read_json(EVIDENCE / file)
        for name, file in {
            "benchmark": "benchmark/results.json",
            "smoke": "raw_smoke.json",
            "trace": "F_trace.json",
            "b2": "H_B2_static_features.json",
            "b3": "H_B3_sketch.json",
        }.items()
    }
    data["registry"] = read_json(ROOT / "outputs/model_registry_manifest.json")
    model = joblib.load(ROOT / "outputs/model.joblib")
    data["features"] = model["full_features"]
    data["importance"] = dict(zip(data["features"], model["rf_multiclass"].feature_importances_))
    data["clusters"] = model["cluster_features"]
    log = (EVIDENCE / "full_suite.log").read_text(encoding="utf-8")
    match = re.search(r"(\d+) passed in ([\d.]+)s", log)
    data["tests"] = f"{match[1]} passed" if match else "Xem full_suite.log"
    data["crashes"] = [read_json(p) for p in sorted((EVIDENCE / "tests").glob("crash_*.json"))]
    return data


def flow(path, labels, title):
    fig, ax = plt.subplots(figsize=(12, 2.7))
    n = len(labels)
    for i, label in enumerate(labels):
        x = (i + 0.5) / n
        ax.text(
            x,
            0.5,
            textwrap.fill(label, 17),
            ha="center",
            va="center",
            fontsize=11,
            color=NAVY,
            bbox=dict(boxstyle="round,pad=.65", facecolor="#E0F2FE", edgecolor=SKY),
        )
        if i < n - 1:
            ax.annotate(
                "", ((i + 1.22) / n, 0.5), ((i + 0.78) / n, 0.5), arrowprops=dict(arrowstyle="->", color=SKY, lw=1.5)
            )
    ax.set(xlim=(0, 1), ylim=(0, 1))
    ax.set_title(title, color=NAVY, fontsize=13, pad=12)
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(path, dpi=180, facecolor="white")
    plt.close(fig)


def make_figures(data):
    FIGURES.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "axes.spines.top": False, "axes.spines.right": False})
    flow(
        FIGURES / "story.png",
        [
            "Bước 1\nGPS và cửa sổ",
            "Bước 2\nĐồ thị và nhóm đường",
            "Bước 3\nMô hình đóng băng",
            "Bước 4\nPhiên bản và UI",
        ],
        "Bốn bước của historical replay PoC",
    )
    flow(
        FIGURES / "time.png",
        ["Nguồn [09:30,10:00)", "Đóng cửa sổ lúc 10:00", "Đích [10:00,10:30)", "Revision có thể phát trễ"],
        "Slot kế tiếp không bảo đảm 30 phút báo trước",
    )
    flow(
        FIGURES / "architecture.png",
        [
            "Raw GPS / Producer",
            "Kafka / offset",
            "Mongo ingress / raw",
            "Feature / frozen score",
            "History / latest / UI",
        ],
        "[I] Historical replay trên một host",
    )
    flow(
        FIGURES / "monoid.png",
        ["GPS theo khóa cửa sổ", "Partial tuple theo salt", "Merge 5 đại lượng", "mean = sum / count"],
        "Kết hợp tuple; parity trước khi so thời gian",
    )
    flow(
        FIGURES / "ledger.png",
        ["Durable ingress", "Exact raw identity", "Snapshot / history", "Atomic latest", "Commit offset + 1"],
        "At-least-once và idempotent durable processing",
    )
    fig, ax = plt.subplots(figsize=(9, 3.2))
    names, counts = ["nodes", "streets", "segments", "GPS pings", "windows"], [577967, 5553, 84633, 90938, 33441]
    ax.barh(names, counts, color=SKY)
    ax.set_xscale("log")
    for i, count in enumerate(counts):
        ax.text(count * 1.08, i, f"{count:,}", va="center", fontsize=10)
    ax.set_xlim(1000, 1300000)
    ax.set_xlabel("Số dòng, trục log; các bảng có vai trò khác nhau")
    fig.tight_layout()
    fig.savefig(FIGURES / "inputs.png", dpi=180)
    plt.close(fig)
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.8))
    for ax, names in zip(axes, [("smj", "bhj"), ("unsalted", "salted")]):
        values = [data["benchmark"]["median_seconds"][n] for n in names]
        bars = ax.bar(names, values, color=["#64748B", SKY])
        ax.bar_label(bars, fmt="%.4f s", padding=4)
        ax.set_ylim(0, max(values) * 1.3)
        ax.set_ylabel("Median giây")
    fig.suptitle("Cùng input; 2 warmups + 5 lần đo xen kẽ; local[2]")
    fig.tight_layout()
    fig.savefig(FIGURES / "benchmark.png", dpi=180)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 3.3))
    sample = data["b2"]["sample"]
    ax.axis("off")
    ax.text(0.02, 0.9, "Nguyễn Kiệm · segment 21969", fontsize=17, weight="bold", color=NAVY)
    ax.text(
        0.02,
        0.67,
        f"PageRank: {sample['pagerank_score']:.4f}     Tốc độ TRAIN: {sample['mean_velocity']:.2f} km/h",
        fontsize=13,
    )
    ax.text(
        0.02,
        0.45,
        f"Tỷ lệ tốc độ: {sample['velocity_ratio']:.4f}     TRAIN windows: {sample['train_obs_windows']}",
        fontsize=13,
    )
    ax.text(
        0.02,
        0.17,
        (
            "Tầm quan trọng cấu trúc không xác lập ùn tắc thực tế.\nKhông suy luận nguyên nhân "
            "từ số làn hoặc sự liền vùng."
        ),
        fontsize=12,
        color=MUTED,
    )
    fig.tight_layout()
    fig.savefig(FIGURES / "graph.png", dpi=180)
    plt.close(fig)
    transitions = pd.read_csv(ROOT / "outputs/transition_evaluation.csv")
    fig, ax = plt.subplots(figsize=(9, 3.6))
    x = range(4)
    ax.bar([i - 0.18 for i in x], transitions.persistence_6class_acc, 0.36, label="Persistence", color="#64748B")
    ax.bar([i + 0.18 for i in x], transitions.rf_23d_6class_acc, 0.36, label="RF 23D", color=SKY)
    ax.set_xticks(
        list(x), ["TEST", "61 thay đổi\nngưỡng mặc định", "61 thay đổi\nhiệu chỉnh VAL", ("3,513 cặp\nGPS ngắt quãng")]
    )
    ax.set_ylim(0, 1.1)
    ax.set_ylabel("Accuracy 6 lớp; dữ liệu batch lịch sử")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIGURES / "transition.png", dpi=180)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 3.5))
    bars = ax.bar(["Deliveries", "Raw duy nhất", "Latest windows"], [22, 20, 3], color=["#64748B", SKY, "#15803D"])
    ax.bar_label(bars, padding=4)
    ax.set_ylim(0, 26)
    ax.set_title("Raw smoke: 20 GPS + 1 duplicate + 1 clock control")
    fig.tight_layout()
    fig.savefig(FIGURES / "smoke.png", dpi=180)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 3.3))
    names = [c["stage"].replace("_", " ") for c in data["crashes"]]
    values = [c["broker_committed"] for c in data["crashes"]]
    bars = ax.barh(names, values, color=SKY)
    ax.bar_label(bars, padding=3)
    ax.set_xlim(0, 7)
    ax.set_xlabel("Broker offset sau restart; 6 deliveries trong fixture")
    fig.tight_layout()
    fig.savefig(FIGURES / "crash.png", dpi=180)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 2.1))
    ax.axis("off")
    for i, service in enumerate(("mongo", "kafka")):
        proof = read_json(EVIDENCE / "outages" / f"{service}_recovery.json")
        ax.text(
            0.02,
            0.72 - i * 0.4,
            f"{service.upper()}: service stopped → process exit {proof['failure']['returncode']} "
            f"→ health restored → restart exit {proof['recovered']['returncode']}",
            fontsize=11,
            color=NAVY,
        )
    fig.tight_layout()
    fig.savefig(FIGURES / "outage.png", dpi=180)
    plt.close(fig)


def report_pages(d):
    reg = d["registry"]
    return [
        dict(
            title="Hệ thống giao thông TP Hồ Chí Minh",
            subtitle="Báo cáo tổng kết BDA501 và nghiệm thu Session 9",
            paragraphs=[
                "Chương 1. Bài toán và phạm vi sử dụng",
                (
                    "Trong ví dụ batch lịch sử, một GPS trên Lý Thường Kiệt, đoạn 47407, báo 4 km/h. "
                    "Một bản tin riêng lẻ chưa mô tả toàn bộ dòng xe. Hệ thống gom quan sát thành cửa "
                    "sổ, bổ sung thông tin đường, dự báo slot kế tiếp và trình bày mức độ đầy đủ của "
                    "dữ liệu."
                ),
                (
                    "Bước 1 chuẩn hóa GPS; Bước 2 tạo đặc trưng đồ thị và nhóm đường; Bước 3 dùng mô "
                    "hình đóng băng; Bước 4 lưu các phiên bản dự báo để phục vụ UI. Đây là historical "
                    "replay PoC, chưa phải nguồn GPS thành phố trực tiếp."
                ),
                TEAM,
                f"Giảng viên hướng dẫn: {INSTRUCTOR} · Fall 2026",
            ],
            figure="story.png",
            caption="Hình 1.1. Bốn bước của hệ thống và ranh giới replay.",
        ),
        dict(
            title="1.4. Cửa sổ đích và thời điểm phát",
            paragraphs=[
                (
                    "Một cửa sổ nguồn [09:30,10:00) có thể cung cấp đầu vào khi đóng lúc 10:00. Dự báo "
                    "khi đó nhắm vào [10:00,10:30). Nhãn slot cách nhau 30 phút không tạo ra 30 phút "
                    "cảnh báo trước khi đích bắt đầu."
                ),
                (
                    "Bản tin tới muộn tạo revision mới, kể cả khi khoảng đích đã bắt đầu hoặc kết "
                    "thúc. UI hiển thị issued_at thực tế, run ID, revision, freshness và target "
                    "interval. Kết quả batch trên các tab khác vẫn được gắn nhãn snapshot."
                ),
                (
                    "Proof-of-Scale là mục đích đối chiếu thuật toán trên mẫu dữ liệu. Nó không phải "
                    "kết quả đo cluster scaling hoặc bằng chứng đáp ứng lưu lượng toàn thành phố."
                ),
            ],
            figure="time.png",
            caption="Hình 1.2. Phân biệt nhãn thời gian và thời gian báo trước.",
        ),
        dict(
            title="Chương 2. Dữ liệu và validation",
            paragraphs=[
                (
                    "Năm CSV có hash khớp manifest rerun. Có 577,967 nodes, 5,553 streets, 84,633 "
                    "segments và 90,938 GPS pings. Aggregation toàn nguồn tạo 33,441 cửa sổ 30 phút. "
                    "train.csv là bảng đối chiếu độc lập, không phải nhãn triển khai trực tiếp."
                ),
                (
                    "Các bảng khác nhau có denominator khác nhau. Số cửa sổ không thể dùng thay cho số "
                    "bản tin; số đoạn đường không thể dùng thay cho số quan sát GPS theo thời gian."
                ),
            ],
            figure="inputs.png",
            caption="Hình 2.1. Quy mô các bảng đầu vào và đầu ra aggregation.",
        ),
        dict(
            title="2.4. Khóa, chất lượng và population",
            paragraphs=[
                (
                    "Contract dùng raw_event_id cho từng ping và event_id cho (segment_id,date,period) "
                    "UTC. Hai ping hợp lệ trong cùng slot đều đóng góp aggregate. Duplicate delivery "
                    "cùng raw_event_id không được tăng count."
                ),
                (
                    "Đường engineering lọc motor roads có 90,787 pings và 33,394 windows. Chênh 151 "
                    "pings và 47 windows với đường toàn nguồn là do filter, không phải mất dữ liệu "
                    "ngầm."
                ),
                (
                    "NFC, timezone UTC/ICT, null/range/key checks, quarantine và parity được kiểm thử "
                    "theo source. Khẳng định monoid baseline toàn nguồn thuộc evidence/session9_rerun; "
                    "full-source benchmark mới có two-way exceptAll = 0."
                ),
                (
                    "Raw sự kiện ngoài watermark vẫn được giữ trong Bronze. Invalid schema, định danh "
                    "rỗng và payload xung đột được ghi lý do cách ly trong ingress."
                ),
            ],
            table=(
                ["Population", "Pings", "Windows"],
                [
                    ["Toàn nguồn", "90,938", "33,441"],
                    ["Motor roads", "90,787", "33,394"],
                    ["Khác do filter", "151", "47"],
                ],
            ),
        ),
        dict(
            title="Chương 3. Kiến trúc và runtime",
            paragraphs=[
                (
                    "[I] Producer phát raw GPS vào Kafka, consumer Python ghi durable ingress, exact "
                    "raw events, feature snapshots, forecast history và latest traffic_scores trong "
                    "MongoDB. UI đọc đúng database/run. Spark đảm nhiệm batch ETL và analytics."
                ),
                (
                    "[P] Live city feed, cloud nhiều nút, online learning và điều khiển đèn vẫn là "
                    "thiết kế. Spark file-stream, local replay cũ và kiểm tra publish Kafka cũ là các "
                    "phép thử riêng."
                ),
                (
                    "Runtime build từ repo: Python 3.13.9, Java 17, PySpark 3.5.7, Kafka 4.1.0, "
                    "MongoDB 8.0.38. Không phụ thuộc image/network/container của project khác."
                ),
            ],
            figure="architecture.png",
            caption="Hình 3.1. Luồng triển khai thực; phần cloud vẫn design-only.",
        ),
        dict(
            title="3.4. Monoid và thực thi Spark",
            paragraphs=[
                (
                    "Tuple gồm count, sum_velocity, min_velocity, max_velocity và severe_ping_count. "
                    "Partial aggregates kết hợp theo phép cộng/min/max; mean cuối cùng là sum/count. "
                    "Không lấy trung bình không trọng số của các trung bình con."
                ),
                (
                    "Key salting tạo partial tuple theo salt rồi merge theo khóa gốc. Chỉ diễn giải "
                    "kết quả sau khi xác nhận parity. Hai implementation trả 33,441 windows và không "
                    "có dòng sai khác hai chiều."
                ),
                (
                    "Hai thống kê cũ 687.43x và 1.023x có population khác nhau nên không được dùng để "
                    "kết luận giảm task skew. Exchange nodes cũng không bằng số runtime stages."
                ),
            ],
            figure="monoid.png",
            caption="Hình 3.2. Monoid kết hợp kết quả partial aggregation.",
        ),
        dict(
            title="3.4. Benchmark và analytical queries",
            paragraphs=[
                (
                    "Mỗi cặp thử chỉ đổi join strategy hoặc salting; cùng input, partitions=8, "
                    "AQE=false, input cache và action collect. Có 2 warmups và 5 lượt đo xen kẽ. Raw "
                    "timings, full plans và 335 task records được giữ lại."
                ),
                (
                    "Median SMJ 0.310514s, BHJ 0.261904s; unsalted 0.124255s, salted 0.116502s. Đây là "
                    "phép đo warm local[2], không phải đo city throughput hoặc cluster scaling."
                ),
                (
                    "Query thứ nhất JOIN GPS/segments và GROUP BY street_id. Query thứ hai lọc "
                    "year_month thực trong Gold, chỉ project segment_id và mean_velocity. Full plans "
                    "chỉ ra PartitionFilters/ReadSchema và shuffle. Ví dụ 7,294 windows tháng 08 thuộc "
                    "dữ liệu lịch sử, không phải partition vừa benchmark."
                ),
            ],
            figure="benchmark.png",
            caption="Hình 3.3. Median của hai cặp thí nghiệm kiểm soát.",
        ),
        dict(
            title="Chương 4. Đồ thị và nhóm đường",
            paragraphs=[
                (
                    "PageRank mô tả tầm quan trọng cấu trúc. B2 dùng Nguyễn Kiệm, segment 21969: "
                    "PageRank 2.7313 nhưng tốc độ TRAIN 45.41 km/h, ratio khoảng 0.9082. Không suy ra "
                    "nguyên nhân từ số làn hoặc gán PageRank thành tình trạng ùn tắc."
                ),
                "K-Means dùng feature list thực trong registry/code: "
                + ", ".join(d["clusters"])
                + ". Không có ràng buộc liền vùng địa lý. Gọi kết quả là behavioral/morphological clusters.",
                (
                    "Độ phủ mô hình cho 74,606 đoạn thiếu GPS không chứng minh accuracy cold-start. "
                    "Luật đồng kẹt hoặc trễ có ít cửa sổ chỉ là exploratory hypotheses."
                ),
            ],
            figure="graph.png",
            caption="Hình 4.1. Đối chiếu cấu trúc với quan sát TRAIN ở B2.",
        ),
        dict(
            title="Chương 5. Dự báo và availability",
            paragraphs=[
                (
                    "Dữ liệu có 7,898 cặp exact t+30m; split sau purge gồm 5,494 TRAIN, 1,167 VAL và "
                    "1,182 TEST. Cutoffs: 2020-12-12T08:00:00Z và 2020-12-25T03:30:00Z. 55 cặp biên bị "
                    "purge."
                ),
                (
                    "Online dùng imputer, Vmax, cluster và priors đóng băng; không fit lại. Run pin "
                    "model, static store, Vmax, source code chia sẻ và runtime versions. Scorer dùng "
                    "chung API batch, nhưng neighbor feature online là frozen street prior; accuracy "
                    "cho policy này chưa tái đánh giá."
                ),
                (
                    "B1 gồm 2 origins × 23 features = 46 dòng. Replay availability có cutoff/sequence; "
                    "vận hành lịch sử luôn UNVERIFIED vì thiếu arrival log và issuance history gốc."
                ),
            ],
            table=(["Feature", "RF importance"], [[f, f"{d['importance'][f]:.4f}"] for f in d["features"]]),
        ),
        dict(
            title="5.4. Đánh giá batch và baseline",
            paragraphs=[
                (
                    "Các metric dưới đây thuộc rerun batch lưu sẵn, không phải accuracy online vừa đo. "
                    "RF có accuracy 0.9450, macro-F1 0.5543, weighted-F1 0.9439, severe F1 0.9409, "
                    "ROC-AUC 0.9882 và severe PR-AUC 0.9471. Persistence accuracy 0.9484 cao hơn RF "
                    "trên TEST đầy mẫu giữ nguyên trạng thái."
                ),
                "Bảng 5.4. RF và Persistence trên cùng TEST.",
            ],
            table=(
                ["Mô hình", "Accuracy", "Severe PR-AUC"],
                [
                    ["Persistence", "0.9484", "0.9367"],
                    ["RF 23D", "0.9450", "0.9471"],
                    ["Decision Tree 23D", "0.9425", "0.9327"],
                ],
            ),
            after=[
                (
                    "Bảng 5.5. Decision Tree 23D có precision 0.9362 và recall 0.9514. Metric này khác "
                    "RF; không trộn các hàng model."
                ),
                (
                    "Ranking tốt chưa chứng minh cảnh báo sớm đủ lead time. Target phải khác dữ liệu "
                    "cửa sổ nguồn và được đánh giá theo đúng forecast issuance contract."
                ),
            ],
            figure="roc",
            caption="Hình 5.1. ROC batch lịch sử; không đánh giá policy online mới.",
        ),
        dict(
            title="5.4. Khi trạng thái thay đổi",
            paragraphs=[
                (
                    "Trên 61 mẫu đổi LOS, RF mặc định đúng 2 / 61; ngưỡng hiệu chỉnh VAL đúng 9 / 61. "
                    "Trong 8 ca từ không severe sang severe, hard recall là 0 / 8. Mô hình chưa chứng "
                    "minh phát hiện tốt các ca onset hiếm."
                ),
                (
                    "Tỷ lệ risk score 9.35x và AUC onset 0.8136 là thống kê ranking lịch sử. Chúng "
                    "không thay thế recall 0 / 8 và không tạo một cam kết vận hành."
                ),
                (
                    "3,513 cặp GPS ngắt quãng là regime khác exact t+30m. Không dùng accuracy của "
                    "regime này để diễn giải accuracy cho 74,606 đoạn cold-start."
                ),
            ],
            figure="transition.png",
            caption="Hình 5.2. Persistence và RF theo regime cùng denominator.",
        ),
        dict(
            title="Chương 6. Streaming bền vững",
            paragraphs=[
                (
                    "Consumer tắt auto-commit. Mỗi delivery có khóa (run_id,topic,partition,offset). "
                    "Exact raw identity quyết định duplicate; nhãn is_duplicate/lateness_bucket không "
                    "đi vào logic."
                ),
                (
                    "Ingress/raw/snapshot/history được ghi acknowledged journaled w=1,j=true. Worker "
                    "drain pending ledger trước khi poll Kafka. Offset từng partition chỉ commit sau "
                    "durable writes và xử lý của record đó."
                ),
                (
                    "Retry dùng dữ liệu tới sequence cutoff cố định. Snapshot và history insert-once "
                    "được đọc lại làm nội dung canonical. Latest cập nhật revision bằng atomic "
                    "conditional pipeline."
                ),
            ],
            figure="ledger.png",
            caption="Hình 6.1. Các ranh giới durable trước broker commit.",
        ),
        dict(
            title="6.2. Raw replay và phiên bản",
            paragraphs=[
                f"Raw smoke dùng 20 GPS thực từ segment {
                    d['smoke']['selected_segment']
                }; producer thêm 1 duplicate và 1 replay-clock control. Run: {
                    d[('smoke')][('run_id')]
                }. Có 3 latest source windows trong MongoDB.",
                (
                    "Watermark là max event time quan sát trừ 30 phút. Replay clock đóng cửa sổ; "
                    "beyond-watermark events vẫn vào Bronze và reconciliation cập nhật cả các lag bị "
                    "ảnh hưởng."
                ),
                (
                    "B3 trên đúng stream này: Bloom false positives 0, exact distinct segments 1, HLL "
                    "khoảng 1.00049. Bloom 32.0 KiB và HLL 1.0 KiB chỉ là evaluator/KPI; trạng thái "
                    "exact online không có giới hạn bộ nhớ 33 KiB."
                ),
                (
                    "Số 501 cửa sổ backfill, 3,953 downstream candidate windows và 377 cửa sổ rescored "
                    "thuộc local replay cũ. Số candidate không đếm feature cells thực sự đổi. Không "
                    "dùng chúng làm kết quả raw-smoke Kafka mới."
                ),
            ],
            figure="smoke.png",
            caption="Hình 6.2. Đếm delivery, raw identity và latest windows trong raw smoke.",
        ),
        dict(
            title="6.3. Crash, outage và độ trễ",
            paragraphs=[
                (
                    "Năm vị trí kill thật: before ingress, after ingress, after raw, after forecast và "
                    "before commit. Mỗi fixture hội tụ về offset 6, 4 raw identities, 5 forecast "
                    "revisions. History trước crash vẫn nguyên vẹn. Concurrent stale writes không kéo "
                    "latest lùi revision."
                ),
                (
                    "Outage Mongo và Kafka thực tế ở startup: process lỗi khi service dừng, sau "
                    "restore health và restart thì hội tụ. Không tuyên bố uninterrupted service hoặc "
                    "mid-request outage coverage."
                ),
                (
                    "Độ trễ ingress-to-forecast trong F_trace khác broker-to-ingress và UI freshness. "
                    "Warm Mongo query median/p95 rerun là 0.432/0.565 ms. Số 0.636 ms thuộc phép đo "
                    "query cũ; không phải latency toàn pipeline."
                ),
            ],
            figure="crash.png",
            caption="Hình 6.3. Broker offset sau restart ở năm failpoints.",
            figure2="outage.png",
            caption2="Hình 6.4. Stop/restore/restart trên hai service thực.",
        ),
        dict(
            title="Chương 7. Đóng gói và tái lập",
            paragraphs=[
                (
                    f"7.2. Validation: {d['tests']}; lint E/F/W/C901 theo README. "
                    "Integration Kafka/Mongo không skip. Build/runtime/image logs, full plans, "
                    "raw timings và read-back được lưu trong evidence/session9."
                ),
                (
                    "7.3. Trách nhiệm: Minh Quyền phụ trách dữ liệu/Spark; Thành Đạt phụ trách "
                    "graph/ML; Công Minh phụ trách Kafka/Mongo/UI. Reviewer độc lập đối chiếu "
                    "correctness, crash và claim consistency."
                ),
                "Model/static/Vmax digest prefixes: "
                + ", ".join(
                    reg[k][:12] for k in ("sha256_digest", "static_feature_store_sha256", ("vmax_lookup_sha256"))
                )
                + (
                    ". Các hash đầy đủ và source snapshot nằm trong manifest; restart cùng run không "
                    "cho đổi registry/code/runtime."
                ),
                (
                    "Runbook: docker compose build spark app; docker compose up -d --wait kafka mongo; "
                    "python -m scripts.session9_validate. Dùng run/database namespace riêng; không xóa "
                    "checkpoint để khởi động lại. Mọi lệnh chạy từ repository root."
                ),
                (
                    "Mongo một nút và khóa worker trên volume chung chỉ phù hợp PoC một host. Không có "
                    "bằng chứng HA, partition tolerance hay exactly-once xuyên Kafka/Mongo."
                ),
            ],
        ),
        dict(
            title="7.4. Session 9 Technical Readiness Summary",
            paragraphs=[
                (
                    "G7 là kiểm tra gói bàn giao, không phải tự chấm toàn hệ thống Ready. Trạng thái "
                    "cuối được xác lập bằng artifact_review.json, test/lint logs và manifest, sau khi "
                    "render và rà từng trang/slide."
                ),
                (
                    "Limitations còn lại được công khai: availability vận hành lịch sử UNVERIFIED, "
                    "accuracy policy online chưa đo, crash oracle chưa phủ cross-partition, chưa có "
                    "full replay throughput và chưa có cluster scaling."
                ),
                (
                    "Ba bước tiếp theo: ML/data kiểm tra accuracy policy online trước claim accuracy; "
                    "streaming đo cross-partition/full replay trước SLO; cả nhóm diễn tập giải thích "
                    "raw-to-serve trước buổi bảo vệ."
                ),
            ],
            table=(
                ["Chủ đề", "Kết luận có evidence"],
                [
                    ["Architecture truth", "Raw GPS → Kafka → consumer → model → Mongo → UI"],
                    ["Performance", "Common-input parity; 2 warmups + 5 alternating runs"],
                    ["Validation", "32 tests; 46 B1 rows; historical UNVERIFIED"],
                    ["Operations", "Idempotent durable processing, immutable history, restart convergence"],
                    ["Limitation", "Single-host replay PoC; no accuracy/scaling overclaim"],
                ],
            ),
        ),
    ]


def report(data, pages):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    from xml.sax.saxutils import escape

    fonts = Path("/usr/share/fonts/truetype/dejavu")
    for name, filename in (("G7", "DejaVuSans.ttf"), ("G7Bold", "DejaVuSans-Bold.ttf")):
        pdfmetrics.registerFont(TTFont(name, str(fonts / filename)))
    normal = ParagraphStyle("body", fontName="G7", fontSize=10, leading=15, spaceAfter=10)
    heading = ParagraphStyle("heading", fontName="G7Bold", fontSize=17, leading=23, spaceAfter=16)
    small = ParagraphStyle("small", parent=normal, fontSize=8.1, leading=11, textColor=colors.HexColor(MUTED))
    cell = ParagraphStyle("cell", parent=normal, fontSize=8.7, leading=12, spaceAfter=0)
    story = []
    for number, page in enumerate(pages, 1):
        story.append(Paragraph(escape(page["title"]), heading))
        if page.get("subtitle"):
            story.append(Paragraph(escape(page["subtitle"]), normal))
        story.extend(Paragraph(escape(p), normal) for p in page["paragraphs"])
        if page.get("table"):
            headers, rows = page["table"]
            values = [[Paragraph(escape(str(v)), cell) for v in row] for row in [headers, *rows]]
            table = Table(values, colWidths=[(A4[0] - 90) / len(headers)] * len(headers), repeatRows=1)
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E0F2FE")),
                        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#CBD5E1")),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("TOPPADDING", (0, 0), (-1, -1), 3),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ]
                )
            )
            story.extend([table, Spacer(1, 12)])
        story.extend(Paragraph(escape(p), normal) for p in page.get("after", []))
        if page.get("figure"):
            path = ROOT / "outputs/roc_curve.png" if page["figure"] == "roc" else FIGURES / page["figure"]
            with PILImage.open(path) as image:
                w, h = image.size
            width = 490
            height = width * h / w
            if height > 210:
                width *= 210 / height
                height = 210
            story.extend([Image(str(path), width=width, height=height), Paragraph(escape(page["caption"]), small)])
        if page.get("figure2"):
            story.extend(
                [
                    Image(str(FIGURES / page["figure2"]), width=490, height=110),
                    Paragraph(escape(page["caption2"]), small),
                ]
            )
        if number < len(pages):
            story.append(PageBreak())

    def footer(canvas, doc):
        canvas.setFont("G7", 8)
        canvas.setFillColor(colors.HexColor(MUTED))
        canvas.drawString(45, 25, "BDA501 · HCMC Smart Traffic · evidence/session9")
        canvas.drawRightString(A4[0] - 45, 25, str(doc.page))

    SimpleDocTemplate(
        str(ROOT / "HCMC_Smart_Traffic_Final_Report.pdf"),
        pagesize=A4,
        leftMargin=45,
        rightMargin=45,
        topMargin=42,
        bottomMargin=45,
        title="BDA501 Final Report and Session 9 Evidence",
        author=TEAM,
    ).build(story, onFirstPage=footer, onLaterPages=footer)
    md = []
    for page in pages:
        md.extend(["## " + page["title"], "", *[p + "\n" for p in page["paragraphs"]]])
        if page.get("table"):
            headers, rows = page["table"]
            md.extend(["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)])
            md.extend("| " + " | ".join(str(v) for v in row) + " |" for row in rows)
        md.extend(page.get("after", []))
        if page.get("caption"):
            md.append(page["caption"])
        if page.get("caption2"):
            md.append(page["caption2"])
        md.append("")
    text = "# Báo cáo tổng kết và đặc tả Session 9\n\n" + "\n".join(md)
    (ROOT / "product/00-proposal/project-proposal.md").write_text(text, encoding="utf-8")
    (ROOT / "TECHNICAL_NOTE.md").write_text(text, encoding="utf-8")


def slides(data):
    from pptx import Presentation
    from pptx.util import Inches
    from scripts.slide_renderer import DualSlide, _pptx_textbox

    prs = Presentation()
    prs.slide_width = Inches(16)
    prs.slide_height = Inches(9)
    prs.core_properties.title = "BDA501 HCMC Smart Traffic Final Presentation"
    prs.core_properties.author = TEAM
    topics = [
        (
            "HCMC Smart Traffic",
            "Historical replay PoC và Session 9",
            "Bài toán",
            [
                "Giám sát cửa sổ GPS và dự báo slot kế tiếp.",
                "Mẫu 84,633 đoạn đường; chưa có live city feed.",
                "Giảng viên: Đặng Trần Khánh.",
            ],
            "Nhóm thực hiện",
            TEAM.split(" · "),
            "story.png",
        ),
        (
            "1. Bài toán và giới hạn của dữ liệu",
            "Độ phủ không phải accuracy",
            "Mẫu dữ liệu",
            [
                "90,938 GPS pings tạo 33,441 cửa sổ.",
                "7,898 cặp exact t+30m; không ghép next-observed thay thế.",
                "74,606 đoạn không có GPS trong mẫu; LOS thực tế chưa biết.",
            ],
            "Phạm vi sử dụng",
            [
                "Proof-of-Scale là mục tiêu đối chiếu thuật toán trên mẫu.",
                "Kết quả không chứng minh lưu lượng toàn thành phố.",
                "Slot kế tiếp không bảo đảm 30 phút báo trước.",
            ],
            "inputs.png",
        ),
        (
            "2. Cơ sở phương pháp và đối chiếu",
            "Lý thuyết đi cùng phép kiểm tra",
            "Batch analytics",
            [
                "Monoid kết hợp count, sum, min, max và severe count.",
                "Spark SQL: physical strategy, shuffle và Parquet pruning.",
                "PageRank là cấu trúc; K-Means là nhóm hành vi/hình thái.",
            ],
            "Model và hệ thống",
            [
                "RF so với Persistence trên cùng TEST.",
                "NoSQL phục vụ per-event/version access pattern.",
                "At-least-once và idempotency; không gán exactly-once Kafka/Mongo.",
            ],
            None,
        ),
        (
            "3. Kiến trúc triển khai thực",
            "[I] Replay một host; [P] live city và cloud nhiều nút",
            "Đường online",
            [
                "Producer → Kafka → Python consumer → Mongo ingress/raw.",
                "Cutoff-bounded features → frozen model → history/latest → UI.",
                "Spark là batch ETL/analytics; file stream là demo riêng.",
            ],
            "Runtime và giới hạn",
            [
                "Python 3.13.9 · Java 17 · Spark 3.5.7.",
                "Kafka 4.1.0 · MongoDB 8.0.38.",
                "Một worker/run; shared checkpoint volume; Mongo một nút.",
            ],
            "architecture.png",
        ),
        (
            "4. Dữ liệu và bảo toàn khóa",
            "Đối chiếu cùng population",
            "Data validation",
            [
                "Toàn nguồn: 90,938 pings → 33,441 windows.",
                "Motor roads: 90,787 pings → 33,394 windows.",
                "151 pings / 47 windows khác do filter; không là silent drops.",
            ],
            "Thời gian và identity",
            [
                "Cửa sổ UTC [start,end); ICT là calendar/display.",
                "raw_event_id phân biệt pings; event_id phân biệt windows.",
                "Null/range/schema và identity conflicts vào quarantine.",
            ],
            None,
        ),
        (
            "5. Monoid và key salting",
            "Parity trước benchmark",
            "Tuple có thể kết hợp",
            [
                "count và sum cộng; min/max kết hợp tương ứng.",
                "Severe count cộng; mean cuối = sum / count.",
                "Không lấy mean không trọng số của partial means.",
            ],
            "Bằng chứng và giới hạn",
            [
                "Salted/unsalted trả cùng 33,441 windows; exceptAll hai chiều = 0.",
                "687.43x và 1.023x là hai population khác nhau.",
                "Không kết luận giảm task skew từ hai thống kê đó.",
            ],
            "monoid.png",
        ),
        (
            "6. PageRank và behavioral clusters",
            "Cấu trúc không xác lập ùn tắc",
            "Card B2",
            [
                "Nguyễn Kiệm 21969: PageRank 2.7313.",
                "TRAIN speed 45.41 km/h; ratio 0.9082; 22 TRAIN windows.",
                "Không suy luận quan hệ nhân quả từ số làn.",
            ],
            "K-Means",
            [
                "Dùng 7 features thực trong code/registry.",
                "Không có ràng buộc liền vùng; không gọi là vùng địa lý.",
                "Coverage cold-start không chứng minh accuracy.",
            ],
            "graph.png",
        ),
        (
            "7. Benchmark và query plans",
            "2 warmups + 5 lượt đo xen kẽ",
            "Kiểm soát thí nghiệm",
            [
                "Cùng input, 8 partitions, AQE=false, input cache và collect.",
                "Join: SMJ 0.310514s; BHJ 0.261904s median.",
                "Aggregate: unsalted 0.124255s; salted 0.116502s median.",
            ],
            "Query và phạm vi",
            [
                "Join/GROUP BY street; filtered Parquet projection.",
                "Full plans và 335 task records; chỉ đo local[2].",
                "7,294 August windows là ví dụ Gold lịch sử.",
            ],
            "benchmark.png",
        ),
        (
            "8. Mô hình đóng băng và feature cutoff",
            "23 features; không fit online",
            "Batch model",
            [
                "5,494 TRAIN / 1,167 VAL / 1,182 TEST; có purge.",
                "RF accuracy 0.9450; ROC-AUC 0.9882; severe PR-AUC 0.9471.",
                "Đây là metric batch lưu sẵn; chưa đo accuracy policy online.",
            ],
            "Card B1",
            [
                "2 origins × 23 features = 46 dòng; cutoff/sequence được lưu.",
                "Historical availability: UNVERIFIED.",
                "Pin artifact, shared code và runtime; đổi contract phải tạo run mới.",
            ],
            None,
        ),
        (
            "9A. Baseline và đánh giá cùng TEST",
            "RF không vượt Persistence ở mọi metric",
            "So sánh công bằng",
            [
                "Persistence accuracy 0.9484; RF 0.9450.",
                "RF severe PR-AUC 0.9471; macro-F1 0.5543.",
                "Tách metric toàn TEST khỏi subset đổi trạng thái.",
            ],
            "Điều chưa được chứng minh",
            [
                "Không suy accuracy cold-start từ độ phủ 84,633 đoạn.",
                "Không dùng batch AUC để chứng minh online policy mới.",
                "Neighbor online là frozen street prior; policy batch khác.",
            ],
            "roc",
        ),
        (
            "9B. Khi giao thông đổi trạng thái",
            "Công khai giới hạn onset hiếm",
            "Kết quả batch lịch sử",
            [
                "61 thay đổi: mặc định 2 / 61; hiệu chỉnh VAL 9 / 61.",
                "8 onset severe: hard recall 0 / 8.",
                "Risk ratio 9.35x là ranking lịch sử, không thay recall.",
            ],
            "Regime khác nhau",
            [
                "3,513 cặp GPS ngắt quãng có denominator riêng.",
                "Slot-label +30m không phải 30 phút advance warning.",
                "Giữ origin, input cutoff, issued_at và target interval.",
            ],
            "transition.png",
        ),
        (
            "10. Consumer bền vững và sketches",
            "Exact identity quyết định duplicate",
            "Durable processing",
            [
                "Tắt auto-commit; journaled ingress trước commit.",
                "Bỏ/đảo simulation labels không đổi kết quả.",
                "History append-only; latest có atomic revision guard.",
            ],
            "Card B3 và provenance",
            [
                "Raw smoke: 20 GPS + 1 duplicate + 1 clock; 3 latest windows.",
                "Bloom 32.0 KiB, HLL 1.0 KiB chỉ evaluator/KPI.",
                "Local replay cũ: 501 late windows; 3,953 downstream candidates; 377 rescored windows.",
            ],
            "ledger.png",
        ),
        (
            "11. Crash và service recovery",
            "Năm kill positions; outage ở startup",
            "Process crash",
            [
                "Trước/sau ingress, sau raw, sau forecast, trước commit.",
                "Fixture hội tụ: offset 6, 4 unique raw, 5 revisions.",
                "History giữ nguyên; stale writes không làm latest lùi.",
            ],
            "Outage và latency",
            [
                "Dừng Mongo/Kafka thật; restore health rồi restart hội tụ.",
                "Warm query rerun p95 0.565 ms; 0.636 ms là query cũ.",
                "Không gọi query latency là end-to-end latency hoặc SLO.",
            ],
            "crash.png",
        ),
        (
            "12. Trace raw GPS đến dashboard",
            "Cùng một run xuyên broker, model và UI",
            "Raw replay thực",
            [
                f"Segment {data['smoke']['selected_segment']}: 20 pings từ CSV.",
                "Broker offset → raw identity → snapshot → model → Mongo.",
                "UI gắn run, revision, issuance, target và freshness.",
            ],
            "Nguồn hiển thị",
            [
                "Bảng vận hành đọc run-scoped MongoDB.",
                "Các ví dụ batch còn lại có nhãn snapshot lịch sử.",
                "No live city feed; chưa đo browser render latency.",
            ],
            "ui",
        ),
        (
            "13. Giới hạn và hướng tiếp theo",
            "Công khai phạm vi thay cho all-Ready",
            "Những điều còn UNVERIFIED",
            [
                "Availability vận hành lịch sử và accuracy policy online.",
                "Cross-partition crash parity; full online replay throughput.",
                "Cluster scaling, HA, network partition và live city feed.",
            ],
            "Ba hành động tiếp theo",
            [
                "ML/data: kiểm tra accuracy policy trước claim vận hành.",
                "Streaming: cross-partition/full replay trước SLO.",
                "Cả nhóm: diễn tập giải thích raw → serve và các trade-offs.",
            ],
            None,
        ),
        (
            "Session 9 Technical Readiness Summary",
            "Bản tóm tắt nghiệm thu theo evidence A–I",
            "Architecture · Performance · Validation",
            [
                "Historical raw replay xuyên Kafka → consumer → model → Mongo → UI.",
                "Common-input parity; 2 warmups + 5 alternating measurements.",
                f"{data['tests']}; 46 B1 rows; historical UNVERIFIED.",
            ],
            "Operations · Limitation",
            [
                "Durable idempotency, immutable revisions, atomic latest và restart recovery.",
                "Single-host replay; chưa chứng minh online accuracy/city scaling.",
                "G7 chốt bằng render review, source manifest và test/lint logs.",
            ],
            None,
        ),
    ]
    notes = []
    for index, (title, subtitle, h1, b1, h2, b2, figure) in enumerate(topics, 1):
        slide = DualSlide(prs)
        if index == 1:
            _pptx_textbox(slide.slide, 0.65, 0.55, 14.6, 0.6, title, 29, NAVY, bold=True)
            _pptx_textbox(slide.slide, 0.65, 1.3, 14.6, 0.5, subtitle, 19, SKY)
        else:
            active = {2: 0, 3: 0, 4: 1, 5: 1, 6: 2, 7: 2, 8: 3, 9: 2, 10: 4, 11: 4, 12: 3, 13: 3, 14: 5, 15: 5, 16: 5}[
                index
            ]
            slide.add_chrome(index, 16, active, "BDA501 · SESSION 9", title, subtitle)
        if index == 16:
            rows = [
                ["Architecture truth", b1[0]],
                ["Performance", b1[1]],
                ["Validation", b1[2]],
                ["Operations", b2[0]],
                ["Limitation", b2[1]],
            ]
            slide.add_table(
                0.55, 2.15, 14.9, 5.8, ["Chủ đề", "Bằng chứng và phạm vi"], rows, [0.22, 0.78], font_size=13
            )
        elif figure:
            slide.add_card(0.55, 2.1, 7.1, 2.95, h1, b1, theme="sky_strong", body_size=14)
            slide.add_card(0.55, 5.25, 7.1, 2.95, h2, b2, body_size=14)
            path = (
                ROOT / "outputs/roc_curve.png"
                if figure == "roc"
                else EVIDENCE / "F_ui.png"
                if figure == "ui"
                else FIGURES / figure
            )
            with PILImage.open(path) as image:
                w, h = image.size
            iw = min(7.3, 5.7 * w / h)
            ih = iw * h / w
            picture = slide.slide.shapes.add_picture(
                str(path),
                Inches(8.0 + (7.3 - iw) / 2),
                Inches(2.2 + (5.7 - ih) / 2),
                width=Inches(iw),
                height=Inches(ih),
            )
            if figure == "ui":
                picture.crop_top = 0.27
                picture.crop_bottom = 0.28
                picture.height = Inches(ih * 0.45)
                picture.top = Inches(2.2 + (5.7 - ih * 0.45) / 2)
        else:
            slide.add_card(0.55, 2.1, 14.9, 2.8, h1, b1, theme="sky_strong", body_size=14)
            slide.add_card(0.55, 5.15, 14.9, 2.8, h2, b2, body_size=14)
        speaker = ["Nguyễn Tạ Minh Quyền", "Nguyễn Thành Đạt", "Phạm Công Minh"][(index - 1) // 6 if index < 13 else 2]
        note = f"Nguồn: evidence/session9 A–I; preserved batch: evidence/session9_rerun. {speaker}. " + " ".join(
            b1 + b2
        )
        slide.slide.notes_slide.notes_text_frame.text = note
        notes.append((title, speaker, b1 + b2, note))
        plt.close(slide.fig)
    prs.save(ROOT / "HCMC_Smart_Traffic_Final_Presentation.pptx")
    return notes


def manual_document():
    from docx import Document
    from docx.shared import Cm, Pt, RGBColor
    from docx.oxml.ns import qn

    document = Document(str(EVIDENCE / "g7_archive/BDA501_Session9_Lab_Manual_G03.docx"))
    body = document._element.body
    for child in list(body):
        if child.tag != qn("w:sectPr"):
            body.remove(child)
    section = document.sections[0]
    section.top_margin = section.bottom_margin = Cm(1.8)
    section.left_margin = section.right_margin = Cm(2)
    for name in ["Normal", "Title", "Subtitle", "Heading 1", "Heading 2", "Heading 3"]:
        style = document.styles[name]
        style.font.name = "DejaVu Sans"
        style.font.color.rgb = RGBColor(0, 0, 0)
        if name == "Normal":
            style.font.size = Pt(10)
            style.paragraph_format.space_after = Pt(7)
    title_pr = document.styles["Title"]._element.pPr
    if title_pr is not None:
        for border in list(title_pr.findall(qn("w:pBdr"))):
            title_pr.remove(border)
    for header in (section.header, section.first_page_header, section.even_page_header):
        for t in header.tables:
            t.autofit = False
            width = (section.page_width - section.left_margin - section.right_margin) // len(t.columns)
            for column in t.columns:
                column.width = width
            for row in t.rows:
                for cell in row.cells:
                    cell.width = width
    return document


def manual(data):
    from docx.shared import Pt
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from docx.opc.constants import RELATIONSHIP_TYPE as RT

    document = manual_document()

    def p(text):
        document.add_paragraph(text)

    def title(text):
        document.add_heading(text, 1)

    def link(cell, text, target):
        paragraph = cell.paragraphs[0]
        hyperlink = OxmlElement("w:hyperlink")
        hyperlink.set(qn("r:id"), document.part.relate_to(target, RT.HYPERLINK, is_external=True))
        run = OxmlElement("w:r")
        node = OxmlElement("w:t")
        node.text = text
        run.append(node)
        hyperlink.append(run)
        paragraph._p.append(hyperlink)

    def table(headers, rows, links=False):
        t = document.add_table(rows=1, cols=len(headers))
        t.style = "Table Grid"
        for c, text in zip(t.rows[0].cells, headers):
            c.text = text
        for values in rows:
            cells = t.add_row().cells
            for i, value in enumerate(values):
                if links and i == len(values) - 1:
                    link(cells[i], value, "evidence/session9/" + value)
                else:
                    cells[i].text = str(value)
        for row in t.rows:
            for cell in row.cells:
                for paragraph in cell.paragraphs:
                    for run in paragraph.runs:
                        run.font.size = Pt(8.5)
        return t

    document.add_paragraph("Session 9 technical audit", "Title")
    p("Integrated Big Data Analytics and Final Project Readiness · cập nhật 18/11/2026")
    p("HCMC Smart Traffic · BDA501 · FSB FPT University · Instructor: " + INSTRUCTOR)
    p(TEAM)
    p(
        (
            "Kết luận: historical replay PoC có đường Kafka → Python consumer → frozen scoring "
            "→ MongoDB → UI. Gói bàn giao được kiểm tra theo evidence; không tự chấm 95/100 và "
            "không xác nhận all-Ready cho vận hành lịch sử."
        )
    )
    p(
        (
            "Bản trước sửa được lưu nguyên tại evidence/session9/g7_archive. session9_original "
            "và session9_rerun giữ nguyên provenance."
        )
    )
    title("1 Alignment and evidence standard")
    p(
        (
            "Core rule: mỗi câu trả lời phải có command, output/log và diễn giải. [I] "
            "implemented, [P] proposed/design-only; N/A là không dùng. Đây là project Card B; "
            "các Card A/C–H không áp dụng."
        )
    )
    p(
        (
            "Nguồn sự thật: evidence/session9/README.md, I_readiness.md, manifest.json và "
            "artifact_review.json. Trạng thái G7 được chốt sau render review; các giới hạn "
            "G1/G3/G5/G6 vẫn phải công khai."
        )
    )
    document.add_page_break()
    title("2 End to end integration truth audit")
    p(
        (
            "Deliverable A: raw GPS producer, broker Kafka, durable ingress ledger, exact raw "
            "identities, cutoff features, frozen registry, history/latest Mongo và UI "
            "run-scoped đều [I]. Spark [I] cho batch. Live city feed, multi-node cloud và điều "
            "khiển đèn là [P]."
        )
    )
    trace = data["trace"]
    raw = trace["raw_event"]
    score = trace["mongo_latest"]
    table(
        ["Stage", "Bằng chứng thực"],
        [
            ["Raw source", f"segment {raw['segment_id']}; raw_event_id {raw['raw_event_id']}"],
            [
                "Broker",
                f"topic {trace['broker_ingress']['topic']}; partition {trace['broker_ingress']['partition']}; offset {
                    trace[('broker_ingress')][('offset')]
                }",
            ],
            ["Feature/model", f"snapshot {score['feature_snapshot_id']}; 23 features"],
            ["Mongo/UI", f"event_id {score['event_id']}; revision {score['revision']}; run {score['run_id']}"],
        ],
    )
    p(
        (
            "Challenge answers: Kafka có vai trò buffer/offset trong replay; batch vẫn chạy "
            "khi bỏ Kafka nhưng mất broker-backed evidence. Online learning N/A. Khi worker "
            "dừng, pending ledger phục hồi; Mongo/Kafka outage startup được kiểm thử bằng "
            "stop/restore/restart. Không khẳng định uptime liên tục."
        )
    )
    p(
        (
            "Deletion test Mongo (design-only): Parquet có thể phục vụ snapshot nhưng mất "
            "per-event versioned upsert và indexed access pattern. Chưa đo latency thay thế; "
            "chi phí compaction và snapshot consistency cần thiết kế thêm."
        )
    )
    p("Evidence: A_architecture.md, F_trace.json, raw_smoke.json, F_ui.png, outages/*.json.")
    document.add_page_break()
    title("3 Performance and scalability lab")
    p(
        (
            "Đổi một yếu tố mỗi cặp: SMJ/BHJ hoặc unsalted/salted. Cùng input, partitions=8, "
            "AQE=false, input cache và action collect. 2 warmups + 5 measured rounds xen kẽ."
        )
    )
    table(["Variant", "Median seconds"], [[k, f"{v:.6f}"] for k, v in data["benchmark"]["median_seconds"].items()])
    p(
        (
            "Join có 90,938 rows mỗi variant; aggregation có 33,441 windows; two-way "
            "exceptAll=0. 335 task records và full plans được lưu. Không coi 687.43x và 1.023x "
            "khác population là giảm task skew. Exchange nodes không phải runtime stages."
        )
    )
    p(
        (
            "Command: docker compose run --rm spark python -m scripts.session9_benchmark. "
            "Evidence: B_benchmark.md và benchmark/results.json. Chỉ là warm local[2]; chưa "
            "chứng minh cluster scaling hay full online throughput."
        )
    )
    document.add_page_break()
    title("4 Data validation and ML evaluation")
    p(
        (
            "Input hashes của đủ 5 CSV khớp rerun. All-source 90,938 → 33,441; motor-road "
            "90,787 → 33,394; khác do filter. Independent baseline lưu provenance rerun; "
            "benchmark mới xác nhận parity salted/unsalted."
        )
    )
    p(
        (
            "Mô hình batch: 7,898 cặp exact t+30m; 5,494 TRAIN / 1,167 VAL / 1,182 TEST; purge "
            "55 cặp biên. Artifact cutoff dùng TRAIN và VAL vì model selection có VAL. Online "
            "không fit lại."
        )
    )
    p(
        (
            "RF accuracy 0.9450, ROC-AUC 0.9882, severe PR-AUC 0.9471. Persistence accuracy "
            "0.9484 cao hơn RF. 61 thay đổi: mặc định 2/61, hiệu chỉnh VAL 9/61; onset hard "
            "recall 0/8. Coverage cold-start không là accuracy."
        )
    )
    p(
        (
            "Mỗi feature snapshot chỉ dùng raw events đã nhận tới sequence/input cutoff. "
            "Online neighbor dùng frozen street prior, khác batch policy; accuracy online chưa "
            "được tái đánh giá. Historical availability vẫn UNVERIFIED."
        )
    )
    p("Evidence: D_data_validation.md, E_model_validation.md, H_B1_availability.csv và full_suite.log.")
    document.add_page_break()
    title("5 NoSQL and streaming audit")
    p(
        (
            "Access pattern: per-event latest score và history theo run/event/revision, cộng "
            "query arterial alerts. Unique keys tách run; latest dùng atomic conditional "
            "pipeline chống stale/concurrent write."
        )
    )
    p(
        (
            "Delivery ledger có khóa (run_id,topic,partition,offset). Raw identity exact "
            "unique trong run. Bloom chỉ hỗ trợ evaluator; HLL chỉ KPI. Bỏ/đảo is_duplicate và "
            "lateness_bucket không đổi processing."
        )
    )
    p(
        (
            "Watermark = max event time quan sát − 30 phút. Equality được chấp nhận; một "
            "microsecond trước watermark là beyond. Late events vẫn vào Bronze và "
            "reconciliation có thể tạo revision mới; retry không tạo thêm revision."
        )
    )
    p(
        (
            "Crash thật ở 5 vị trí hội tụ về offset 6, 4 raw events, 5 history rows trong "
            "fixture một partition. Snapshot/history canonical được đọc lại; run pin "
            "artifacts, shared code và runtime. Không tuyên bố exactly-once Kafka/Mongo."
        )
    )
    p(
        (
            "Warm Mongo query rerun median/p95 0.432/0.565 ms, không phải end-to-end. "
            "Ingress-to-forecast, broker-to-ingress và UI freshness là các đại lượng riêng; "
            "không áp SLO chưa có baseline."
        )
    )
    document.add_page_break()
    title("6 Deployment and reproducibility")
    p(
        (
            "Build từ repo: Python 3.13.9, Java 17, PySpark 3.5.7, Kafka 4.1.0, MongoDB "
            "8.0.38. Broker nội bộ kafka:29092. URI, run/database, topic, registry và "
            "checkpoint qua environment/CLI."
        )
    )
    p(
        (
            "Một host, một worker/run, cùng shared checkpoint volume. Mongo w=1,j=true một nút "
            "không chứng minh HA/partition tolerance. Không xóa checkpoints khi restart. "
            "Consumer rebuild closed windows có chi phí tăng theo state; chưa đo full-dataset "
            "online capacity."
        )
    )
    p(
        (
            "Commands: docker compose build spark app; docker compose up -d --wait kafka "
            "mongo; python -m scripts.session9_validate; python -m scripts.session9_outages. "
            "Dùng namespace session9_tests/session9_outages/session9_online với run ID riêng."
        )
    )
    p(
        (
            "Môi trường dựng tài liệu được ghi trong Dockerfile.artifacts; lệnh build/render "
            "và phiên bản công cụ nằm tại g7_tooling.json. Nội dung và hình ảnh được kiểm tra "
            "trên file xuất thật; nguồn trước cập nhật được giữ trong g7_archive."
        )
    )
    document.add_page_break()
    title("7 Card B seven prompts")
    answers = [
        (
            "Forecast as-of: cửa sổ nguồn [start,end); input cutoff, actual issued_at và "
            "target interval được lưu. Không dùng target-period observations hoặc revisions "
            "tương lai."
        ),
        "Availability: 46 rows = 2 origins × 23 actual features; replay verdict riêng với historical UNVERIFIED.",
        (
            "Sparse GPS: 10,027/84,633 observed segments trong mẫu; zero observation nghĩa là "
            "chưa biết LOS thực. Không suy accuracy cold-start từ coverage."
        ),
        (
            "Graph validity: PageRank là topology. Segment 21969 có PageRank 2.7313 nhưng "
            "TRAIN speed 45.41 km/h; không suy nguyên nhân về làn."
        ),
        (
            "Clusters: dùng feature list thực, không có ràng buộc liền vùng địa lý. Gọi "
            "behavioral/morphological clusters."
        ),
        (
            "Sketch accuracy: cùng 21 ping deliveries trong raw smoke; 1 duplicate, Bloom "
            "false positives 0, exact segments 1, HLL khoảng 1.00049. KPI sketch không đổi "
            "alert/aggregate exact."
        ),
        (
            "Issuance/recovery: append-only history, retry không tăng history, stale latest bị "
            "chặn; kill thật và startup outages có durable read-back."
        ),
    ]
    for i, answer in enumerate(answers, 1):
        p(f"Prompt {i}. {answer}")
    document.add_page_break()
    title("7 Card B mini tests and defense")
    b1 = pd.read_csv(EVIDENCE / "H_B1_availability.csv")
    origins = b1[["origin", "event_id", "input_cutoff", "simulated_input_cutoff"]].drop_duplicates()
    table(
        ["Origin", "event_id", "Replay cutoff"],
        [[r.origin, r.event_id, r.simulated_input_cutoff] for r in origins.itertuples()],
    )
    p(
        (
            "B1: H_B1_availability.csv có source_time, available_at, artifact_cutoff, "
            "input_cutoff và verdict. available_at observed là upper bound từ raw ở sequence "
            "cutoff. Không phục dựng được lịch sử vận hành chỉ từ replay."
        )
    )
    p(
        (
            "B2: H_B2_static_features.json nêu sample_segment_id=21969, feature list và TRAIN "
            "statistics. B3: H_B3_sketch.json so exact/sketch trên cùng raw-smoke stream, "
            "không suy rộng error rate mẫu nhỏ thành toàn bộ dữ liệu."
        )
    )
    p(
        (
            "One-minute defense: Nhóm đã nối raw GPS lịch sử qua broker vào durable ingress, "
            "exact Bronze identities, cutoff-bounded features và frozen score. Mongo giữ "
            "history và latest, UI chọn run và hiển thị revision/target/freshness. Kill thật, "
            "startup outages và controlled Spark benchmarks có log. Historical availability, "
            "online accuracy và distributed scaling còn UNVERIFIED; nhóm không che các giới "
            "hạn bằng all-Ready hay điểm tự chấm."
        )
    )
    document.add_page_break()
    title("8 Final technical completeness checklist")
    items = [
        ("Problem & justification", "Có giới hạn", "A_architecture.md"),
        ("End-to-end architecture", "Có evidence", "F_trace.json"),
        ("Project MapReduce", "Có parity", "D_data_validation.md"),
        ("Spark SQL/DataFrames", "Có plans", "C_queries.md"),
        ("Curated Parquet", "Có provenance", "C_queries.md"),
        ("Two analytical queries", "Có evidence", "C_queries.md"),
        ("Controlled performance", "Local only", "B_benchmark.md"),
        ("ML/analytics", "Batch / online limit", "E_model_validation.md"),
        ("NoSQL serving", "Có read-back", "F_trace.json"),
        ("Streaming", "Có crash fixture", "G_deployment.md"),
        ("Cloud design", "Design-only", "G_deployment.md"),
        ("Validation", "Có logs / limits", "validation.json"),
        ("Reproducibility", "Có manifest", "manifest.json"),
        ("Report/code/deck", "Render review", "artifact_review.json"),
        ("Team contribution", "Có phân công", "I_readiness.md"),
        ("Demo evidence", "Replay only", "F_ui.png"),
    ]
    table(["Required item", "Status", "Evidence"], items, links=True)
    p("Mỗi trạng thái áp dụng đúng phạm vi ở cột Evidence; không diễn giải thành 16/16 vận hành Ready.")
    document.add_page_break()
    title("8 Red flag review")
    rows = [
        ("Không giải thích được tech", "A phân biệt I/P", "A_architecture.md"),
        ("Realtime overclaim", "Historical replay rõ", "G_deployment.md"),
        ("Target từ source feature", "Exact next-slot pairing", "E_model_validation.md"),
        ("Future leakage", "Historical UNVERIFIED", "H_B1_availability.csv"),
        ("Metric không đúng task", "Baseline/regime tách riêng", "E_model_validation.md"),
        ("NoSQL chỉ nhanh", "Per-key/version access", "F_trace.json"),
        ("Không benchmark kiểm soát", "Warm local only", "B_benchmark.md"),
        ("Không tái lập được", "Runtime/log/hash", "manifest.json"),
        ("Trộn implemented/proposed", "I/P tách rõ", "A_architecture.md"),
    ]
    table(["Red flag", "Disposition", "Evidence"], rows, links=True)
    p(
        (
            "Disposition là review kết quả và giới hạn; không phải khẳng định mọi rủi ro đã bị "
            "loại bỏ. G1 historical availability và G3 cross-partition coverage vẫn mở đúng "
            "phạm vi."
        )
    )
    document.add_page_break()
    title("9 Readiness self assessment")
    table(
        ["Criterion", "Weight", "Assessment"],
        [
            ["Accuracy & Correctness", 25, "Không tự chấm; xem D/E, B1 và tests"],
            ["Creativity & Problem-Solving", 15, "Trade-offs theo A/B/G"],
            ["Completeness", 25, "I_readiness và artifact_review"],
            ["Presentation & Expression", 10, "Render review, diễn tập nhóm"],
            ["Application of Theory", 20, "Monoid/Spark/NoSQL/time semantics"],
            ["Timeliness & Requirements", 5, "Do giảng viên đánh giá; không tự xác nhận"],
        ],
    )
    p(
        (
            "Tổng trọng số 100; không công bố điểm 95/100 đã được xác minh. G7 kiểm tra "
            "handoff không thay grade của giảng viên."
        )
    )
    table(
        ["Priority", "Action / Owner", "Due / Status"],
        [
            [1, "ML/data kiểm tra accuracy policy online", "Trước claim operational accuracy / OPEN"],
            [2, "Streaming đo cross-partition/full replay", "Trước SLO/scaling claim / OPEN"],
            [3, "Cả nhóm diễn tập raw-to-serve và giới hạn", "Trước bảo vệ / PLANNED"],
        ],
    )
    document.add_page_break()
    title("10 Handoff and demo")
    table(
        ["Readiness summary", "Supported statement"],
        [
            ["Architecture truth", "Raw replay xuyên Kafka → model → Mongo → UI"],
            ["Performance", "Parity + 2 warmups + 5 alternating local measurements"],
            ["Validation", data["tests"] + "; B1 historical UNVERIFIED"],
            ["Operations", "Exact identity, immutable history, atomic latest, restart recovery"],
            ["Limitation", "Single host; chưa đo online accuracy, full throughput, HA"],
        ],
    )
    p(
        (
            "Summary tích hợp trong slide 16: Session 9 Technical Readiness Summary. Manual "
            "này, report và deck dùng chung source/evidence; kết quả QA nằm trong "
            "artifact_review.json."
        )
    )
    p(
        (
            "Demo: xác nhận run ID raw_smoke.json; mở bảng vận hành UI; đối chiếu F_trace.json "
            "với snapshot/history/latest. Khi Mongo unavailable, run-scoped UI báo unavailable "
            "thay vì lẫn snapshot cũ. Dùng screenshot F_ui.png làm fallback có nhãn."
        )
    )
    p(
        (
            "Final defense principle: giải thích vì sao từng công nghệ có mặt, nêu chính xác "
            "dữ liệu nào đã biết tại cutoff, và giữ các giới hạn chưa kiểm chứng. Mọi checkbox "
            "phải trỏ được tới source/evidence của phiên bản bàn giao."
        )
    )
    document.save(ROOT / "BDA501_Session9_Lab_Manual_G03.docx")


def presentation_script(data, notes, pages):
    text = [
        "# Kịch bản bảo vệ Session 9\n",
        TEAM,
        "Giảng viên: " + INSTRUCTOR,
        "Bộ deck: HCMC_Smart_Traffic_Final_Presentation.pptx và HCMC_Smart_Traffic_Final_Slides.pdf; 16 slides.",
        (
            "Live Demo: localhost:8501 với run ID cấu hình; bản kiểm chứng hiện tại ở "
            "localhost:8502. F_ui.png là fallback replay có nhãn."
        ),
        (
            "Tra cứu nhanh Báo cáo: HCMC_Smart_Traffic_Final_Report.pdf, 7 chương; "
            "evidence/session9/README.md là index A–I."
        ),
    ]
    for index, (title, speaker, bullets, note) in enumerate(notes, 1):
        text.extend(
            [
                f"\n#### Slide {index:02d}: {title}",
                "Người trình bày: " + speaker,
                "Mục tiêu: trình bày evidence đúng denominator, clock và provenance.",
                *["- " + b for b in bullets],
                "Lời thoại: " + " ".join(pages[index - 1]["paragraphs"]),
                (
                    "Điểm phản biện: hỏi kết quả nào là batch lịch sử, kết quả nào là raw replay mới. "
                    "Không dùng số đo này để suy ra live-city capacity, accuracy cold-start hoặc 30 "
                    "phút cảnh báo trước target bắt đầu."
                ),
                (
                    "Nguồn để mở khi cần: evidence/session9, outputs/ablation_results.csv và "
                    "outputs/transition_evaluation.csv; chỉ mở đúng file hỗ trợ claim trên slide."
                ),
            ]
        )
    qa = [
        (
            "Vì sao dùng Kafka? Nó cung cấp broker buffer và partition offsets cho replay. "
            "CSV/Parquet vẫn phục vụ batch nhưng không thay được bằng chứng broker-backed "
            "consumer recovery."
        ),
        (
            "Vì sao Persistence accuracy cao hơn RF? TEST có 1,121/1,182 mẫu giữ nguyên LOS. "
            "So sánh cùng TEST, tách 61 thay đổi và 8 onset; không chỉ chọn metric có lợi."
        ),
        (
            "Mô hình biết gì tại issuance? Chỉ raw đã nhận tới sequence/input cutoff, cùng "
            "artifact đóng băng có cutoff hợp lệ. B1 replay pass không phục hồi arrival log "
            "vận hành lịch sử."
        ),
        (
            "Target +30m nghĩa gì? Slot labels cách nhau 30 phút. Cửa sổ đích bắt đầu khi "
            "nguồn đóng; thời điểm phát thực có thể trễ. Không hứa 30 phút advance warning."
        ),
        (
            "PageRank cao có phải kẹt? Không. B2 segment 21969 có PageRank 2.7313 nhưng tốc độ "
            "TRAIN 45.41 km/h. Không suy luận nguyên nhân về số làn."
        ),
        (
            "K-Means có tạo vùng địa lý? Không có contiguity constraint. Đây là "
            "behavioral/morphological clusters theo feature list thực, không phải vùng không "
            "gian liên tục."
        ),
        (
            "Bloom false positive có làm mất raw? Online quyết định bằng exact Mongo identity. "
            "32.0 KiB Bloom và 1.0 KiB HLL chỉ evaluator/KPI; sample B3 nhỏ không đại diện "
            "full stream."
        ),
        (
            "Crash test đã đo gì? Năm kill positions thật trên một partition; restart về "
            "offset 6, unique raw 4 và forecast history 5. Không suy ra cross-partition parity "
            "hoặc host-power-loss durability."
        ),
        (
            "Latency nào được công bố? Query cũ 0.636 ms là warm query, rerun p95 0.565 ms. "
            "Ingress-to-forecast, broker-to-ingress và UI freshness tách riêng; chưa có "
            "operational SLO."
        ),
        (
            "Điều gì còn mở? Historical availability UNVERIFIED, online feature-policy "
            "accuracy, cross-partition/full throughput, cluster scaling và HA. G7 bàn giao tài "
            "liệu không làm các giới hạn này biến mất."
        ),
    ]
    for i, answer in enumerate(qa, 1):
        text.extend(
            [
                f"\n### Câu {i}:",
                answer,
                (
                    "Để bảo vệ kết luận, mở file evidence tương ứng thay vì nhắc lại một con số không "
                    "có denominator. Nếu câu hỏi nằm ngoài phép thử đã chạy, nêu rõ giới hạn và phép "
                    "thử cần bổ sung."
                ),
            ]
        )
    (ROOT / "PRESENTATION_SCRIPT.md").write_text("\n\n".join(text) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["all", "report", "slides", "manual"], default="all")
    args = parser.parse_args()
    archive()
    data = evidence()
    make_figures(data)
    pages = report_pages(data)
    if args.only in {"all", "report"}:
        report(data, pages)
    if args.only in {"all", "manual"}:
        manual(data)
    if args.only in {"all", "slides"}:
        presentation_script(data, slides(data), pages)
    print("Built", args.only, "artifacts from current Session 9 evidence")


if __name__ == "__main__":
    main()
