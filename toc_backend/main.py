"""FastAPI Entrypoint for the HCMC Smart Traffic Operations Center (`toc-web` :8000)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import threading
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from toc_backend.data_store import TOCDataStore
from toc_backend.pipeline_trace import build_pipeline_trace
from toc_backend.stream_controller import (
    get_live_stream_status,
    reset_live_stream,
    start_live_stream_replay,
)

ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIST = ROOT / "toc_frontend" / "dist"

_STORE_LOCK = threading.Lock()
_STORE_INSTANCE: TOCDataStore | None = None


class StreamStartRequest(BaseModel):
    mode: Literal["story", "stress"] = "story"
    scenario: Literal[
        "noon_transition",
        "cong_truong_quoc_te",
        "morning_rush_dien_bien_phu",
        "morning_rush_val",
        "mega_city_rush",
    ] = "noon_transition"
    max_messages: int = Field(default=200, ge=10, le=3000)
    reset_first: bool = True


def get_store() -> TOCDataStore:
    """Thread-safe double-checked singleton initializer for `TOCDataStore`."""
    global _STORE_INSTANCE
    if _STORE_INSTANCE is None:
        with _STORE_LOCK:
            if _STORE_INSTANCE is None:
                _STORE_INSTANCE = TOCDataStore(root=ROOT)
    return _STORE_INSTANCE


def _resolve_doc_coords(doc: dict[str, Any], coord_map: dict[int, list[float]]) -> list[float] | None:
    """Safely map a live score document's `segment_id` to `[s_lat, s_lon, e_lat, e_lon]`."""
    raw_sid = doc.get("segment_id")
    if raw_sid is None or isinstance(raw_sid, bool):
        return None
    try:
        return coord_map.get(int(raw_sid))
    except (TypeError, ValueError):
        return None


def _attach_stream_coords(res: dict[str, Any], store: TOCDataStore) -> dict[str, Any]:
    """Attach road polyline `coords` to each record in `res["recent_scores"]`."""
    recent = res.get("recent_scores")
    if not isinstance(recent, list):
        return res
    enriched = [
        {**doc, "coords": _resolve_doc_coords(doc, store.coord_map)} if isinstance(doc, dict) else doc
        for doc in recent
    ]
    return {**res, "recent_scores": enriched}


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Pre-load `TOCDataStore` once before accepting concurrent HTTP requests."""
    get_store()
    yield


app = FastAPI(
    title="HCMC Smart Traffic Operations Center API",
    description="FastAPI + ReactJS Real-Time 30-Minute Congestion Forecasting & Big Data Audit Platform (BDA501)",
    version="3.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def api_health() -> dict[str, Any]:
    store = get_store()
    return {
        "status": "ok",
        "service": "hcmc-toc-command-center",
        "is_demo_sample": store.is_demo_sample,
        "windows_loaded": int(len(store.win_df)),
        "segments_indexed": int(len(store.coord_map)),
    }


@app.get("/api/overview")
def api_overview() -> dict[str, Any]:
    return get_store().get_overview()


@app.get("/api/scenarios")
def api_scenarios() -> dict[str, Any]:
    return {"scenarios": get_store().get_scenarios()}


@app.get("/api/frame")
def api_city_frame(
    ict_date: str = Query(default="2021-01-05"),
    vn_hour: float = Query(default=12.0),
    alert_filter: str = Query(default="early_warning"),
    include_live_overlay: bool = Query(default=False),
) -> dict[str, Any]:
    return get_store().get_city_frame(
        ict_date=ict_date,
        vn_hour=vn_hour,
        alert_filter=alert_filter,
        include_live_overlay=include_live_overlay is True,
    )


@app.get("/api/segment/{segment_id}/timeline")
def api_segment_timeline(
    segment_id: int,
    ict_date: str | None = Query(default=None),
    vn_hour: float | None = Query(default=None),
) -> dict[str, Any]:
    try:
        return get_store().get_segment_timeline(segment_id=segment_id, ict_date=ict_date, vn_hour=vn_hour)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/graph-rules")
def api_graph_rules() -> dict[str, Any]:
    return get_store().get_graph_and_rules()


@app.get("/api/clusters-coldstart")
def api_clusters_coldstart(
    segment_id: int | None = Query(default=None),
    limit: int = Query(default=8, ge=3, le=20),
) -> dict[str, Any]:
    return get_store().get_clusters_and_coldstart(target_segment_id=segment_id, limit=limit)


@app.get("/api/infrastructure-audit")
def api_infrastructure_audit() -> dict[str, Any]:
    return get_store().get_infrastructure_audit()


@app.get("/api/pipeline-trace")
def api_pipeline_trace(
    trace_id: str | None = Query(default=None),
    segment_id: int | None = Query(default=None),
    ict_date: str | None = Query(default=None),
    vn_hour: float | None = Query(default=None),
) -> dict[str, Any]:
    try:
        return build_pipeline_trace(
            get_store(),
            trace_id=trace_id,
            segment_id=segment_id,
            ict_date=ict_date,
            vn_hour=vn_hour,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/stream/status")
def api_stream_status(limit: int = Query(default=30, ge=5, le=100)) -> dict[str, Any]:
    eff_limit = limit if isinstance(limit, int) and not isinstance(limit, bool) else 30
    res = get_live_stream_status(limit=eff_limit)
    return _attach_stream_coords(res, get_store())


@app.post("/api/stream/start")
def api_stream_start(req: StreamStartRequest) -> dict[str, Any]:
    return start_live_stream_replay(
        mode=req.mode,
        scenario=req.scenario,
        max_messages=req.max_messages,
        reset_first=req.reset_first,
    )


@app.post("/api/stream/reset")
def api_stream_reset() -> dict[str, Any]:
    return reset_live_stream()


if (FRONTEND_DIST / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="assets")


@app.get("/{full_path:path}", response_model=None)
def serve_react_spa(full_path: str) -> FileResponse | HTMLResponse:
    dist_root = FRONTEND_DIST.resolve()
    target = (dist_root / full_path).resolve()
    if full_path and target.is_relative_to(dist_root) and target.is_file():
        return FileResponse(str(target))
    index_html = dist_root / "index.html"
    if index_html.is_file():
        return FileResponse(str(index_html))
    return HTMLResponse("<h3>HCMC TOC Backend is running. Build `toc_frontend` to view UI.</h3>")
