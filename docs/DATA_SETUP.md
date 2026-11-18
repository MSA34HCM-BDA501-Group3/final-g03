# Thiết lập dữ liệu để tái lập dự án

## Nguồn và quyền sử dụng

Dataset được ghi nhận trong `outputs/metrics.json` là [HCMC Traffic Flow Data](https://www.kaggle.com/datasets/thanhnguyen2612/traffic-flow-data-in-ho-chi-minh-city-viet-nam). Repository không chứa dữ liệu nguồn; hãy tải trực tiếp từ trang dataset, đọc điều khoản và giữ thông tin ghi công theo yêu cầu của chủ sở hữu. Không commit raw data hoặc thông tin xác thực Kaggle.

## Chuẩn bị trên Windows

Tạo thư mục `raw_data/` ở thư mục gốc repository, tải ZIP từ Kaggle và giải nén sao cho năm tệp nằm trực tiếp trong thư mục đó:

```powershell
New-Item -ItemType Directory -Force raw_data
Expand-Archive -Path "$env:USERPROFILE\Downloads\<ten-file-dataset>.zip" -DestinationPath raw_data -Force

$required = @("nodes.csv", "streets.csv", "segments.csv", "segment_status.csv", "train.csv")
$missing = @($required | Where-Object { -not (Test-Path (Join-Path "raw_data" $_)) })
if ($missing.Count -gt 0) {
    throw "Thiếu tệp nguồn trong raw_data/: $($missing -join ', ')"
}
```

Kiểm kê kỳ vọng của bản dữ liệu dùng trong báo cáo:

| Tệp | Số dòng dữ liệu | Kích thước xấp xỉ |
|---|---:|---:|
| `nodes.csv` | 577,967 | 19.44 MB |
| `streets.csv` | 5,553 | 0.20 MB |
| `segments.csv` | 84,633 | 10.29 MB |
| `segment_status.csv` | 90,938 | 3.58 MB |
| `train.csv` | 33,441 | 4.99 MB |

Các con số kiểm kê trong `outputs/file_inventory.csv` là kết quả đã ghi nhận; cần chạy lại pipeline trên bộ dữ liệu đã tải để xác nhận phiên bản dữ liệu cục bộ.

## Chạy và kiểm tra đầu ra

```powershell
python -m pip install -r requirements.txt
docker compose up --build -d kafka mongo spark
python pipeline.py
python baseline.py
pytest -v
ruff check . --select E,F,W,C901 --config "lint.mccabe.max-complexity=14" --config "line-length=120"
```

Sau khi chạy thành công, cần có `data/dataset.csv`, `data/lakehouse/segment_static_features.parquet` và các báo cáo trong `outputs/`, bao gồm `outputs/transition_evaluation.csv`. Dashboard `app.py` đọc dữ liệu đã sinh nên chỉ khởi chạy sau pipeline:

```powershell
docker compose up -d app
```

Hoặc dùng `streamlit run app.py` trong môi trường Python đã cài dependencies. `ui-demo.html` là bản tĩnh độc lập, không cần raw data.

`raw_data/` và `data/` nằm trong `.gitignore` có chủ ý. Bản dựng Spark 3.5.7 dùng `Dockerfile.spark35`; `Dockerfile.spark`/`requirements-spark.txt` là môi trường riêng PySpark 4.2.0 cho `compose.quyen.yml`.
