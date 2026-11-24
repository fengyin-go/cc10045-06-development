"""数据分析管道路由：事件采集、聚合执行、指标快照、分群计算等核心操作入口。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter

from app.database import db
from app.pipeline import (
    check_alert, compute_aggregation, compute_cohort_comparison,
    compute_forecast, compute_funnel, compute_metric_snapshot,
    compute_retention, compute_segment, generate_report,
    ingest_events, precompute_cube, seed_real_data,
)
from app.schemas import MetricSamplingPayload, ReviewFileImportPayload
from app.services import service_for

router = APIRouter(prefix="/api/pipeline", tags=["分析管线"])


@router.post("/seed")
def api_seed_real_data() -> dict:
    """初始化全部种子数据：事件、指标、聚合窗口、分群、漏斗等。"""
    counts = seed_real_data()
    return {"ok": True, "counts": counts}


@router.post("/events/ingest")
def api_ingest_events() -> dict:
    """对所有已启用的事件定义批量摄入模拟事件。"""
    svc = service_for("event_tracker")
    result = svc.ingest_all_enabled()
    return result


@router.post("/aggregation/run-all")
def api_run_all_aggregations() -> dict:
    """对所有已启用聚合窗口执行计算。"""
    svc = service_for("realtime_agg")
    result = svc.run_all_active()
    return result


@router.post("/aggregation/{window_id}/run")
def api_run_aggregation(window_id: int) -> dict:
    """对指定聚合窗口执行单次计算。"""
    svc = service_for("realtime_agg")
    return svc.run_aggregation(window_id)


@router.post("/metrics/compute-all")
def api_compute_all_metrics() -> dict:
    """对所有指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_all()


@router.post("/metrics/{metric_id}/snapshot")
def api_compute_snapshot(metric_id: int) -> dict:
    """对指定指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_snapshot(metric_id)


@router.post("/segments/{segment_id}/compute")
def api_compute_segment(segment_id: int) -> dict:
    """对指定分群执行计算。"""
    svc = service_for("segment_engine")
    return svc.run_computation(segment_id)


@router.post("/funnels/{funnel_id}/compute")
def api_compute_funnel(funnel_id: int) -> dict:
    """对指定漏斗执行分析计算。"""
    svc = service_for("funnel_analyzer")
    return svc.run_computation(funnel_id)


@router.post("/retention/{task_id}/compute")
def api_compute_retention(task_id: int) -> dict:
    """对指定留存任务执行计算。"""
    svc = service_for("retention_analysis")
    return svc.run_computation(task_id)


@router.post("/forecast/{task_id}/compute")
def api_compute_forecast(task_id: int) -> dict:
    """对指定预测任务执行计算。"""
    svc = service_for("forecast_model")
    return svc.run_forecast(task_id)


@router.post("/comparison/{comparison_id}/compute")
def api_compute_comparison(comparison_id: int) -> dict:
    """对指定分群对比执行计算。"""
    svc = service_for("cohort_compare")
    return svc.run_comparison(comparison_id)


@router.post("/cube/{cube_id}/precompute")
def api_precompute_cube(cube_id: int) -> dict:
    """对指定Cube执行预计算。"""
    svc = service_for("cube_builder")
    return svc.run_precomputation(cube_id)


@router.post("/report/{report_id}/generate")
def api_generate_report(report_id: int) -> dict:
    """对指定报告模板执行生成。"""
    svc = service_for("data_report")
    return svc.generate(report_id)


@router.post("/report/{report_id}/issue")
def api_issue_report(report_id: int) -> dict:
    """签发报告：冻结本次实际采用的有效解释（原文件行号+采样编号）和文件遗漏。"""
    svc = service_for("data_report")
    return svc.issue(report_id)


@router.get("/report-outputs/{output_id}/review")
def api_review_report_output(output_id: int) -> dict:
    """回查周报：采用的指标数据与正式解释，以及该文件为什么有遗漏。"""
    svc = service_for("data_report")
    return svc.review_output(output_id)


def _parse_review_csv(csv_text: str) -> list[dict]:
    """解析复核文件 CSV 文本，表头支持 指标/业务时点/解释/行号 或英文字段名。"""
    reader = csv.DictReader(io.StringIO(csv_text))
    rows: list[dict] = []
    for raw in reader:
        rows.append({
            "row_no": raw.get("行号") or raw.get("row_no") or None,
            "metric_name": (raw.get("指标") or raw.get("metric_name") or "").strip(),
            "business_time": (raw.get("业务时点") or raw.get("business_time") or "").strip(),
            "explanation": (raw.get("解释") or raw.get("explanation") or "").strip(),
        })
    return rows


@router.post("/review-files/import")
def api_import_review_file(payload: ReviewFileImportPayload) -> dict:
    """导入复核文件：逐行按指标和业务时点关联采样，保存有效解释和未匹配原因。"""
    rows: list[dict] = [r.model_dump() for r in payload.rows]
    if payload.csv_text and payload.csv_text.strip():
        rows.extend(_parse_review_csv(payload.csv_text))
    svc = service_for("anomaly_analysis")
    return svc.import_review(payload.file_name, rows, payload.uploader)


@router.get("/review-files")
def api_list_review_files() -> dict:
    """列出全部已导入的复核文件批次。"""
    svc = service_for("anomaly_analysis")
    return {"ok": True, "items": svc.list_review_files()}


@router.get("/review-files/{file_id}")
def api_review_file_detail(file_id: int) -> dict:
    """回查复核文件：逐行导入结果与未匹配原因。"""
    svc = service_for("anomaly_analysis")
    return svc.review_file_detail(file_id)


@router.post("/metrics/sample")
def api_register_metric_sampling(payload: MetricSamplingPayload) -> dict:
    """补采：为指标在指定业务时点登记一条采样，返回采样编号。"""
    svc = service_for("metric_store")
    return svc.register_sampling(
        metric_name=payload.metric_name or "",
        metric_id=payload.metric_id,
        snapshot_value=payload.snapshot_value,
        snapshot_time=payload.snapshot_time,
    )


@router.post("/alerts/check-all")
def api_check_all_alerts() -> dict:
    """检查所有已生效告警规则。"""
    svc = service_for("alert_policy")
    return svc.check_all()


@router.get("/analytics/full-pipeline")
def api_full_pipeline() -> dict:
    """执行完整分析链路：事件采集→聚合→指标→分群→漏斗→留存→预测→报告。"""
    steps = {}

    # 1. 事件采集
    svc_events = service_for("event_tracker")
    steps["events"] = svc_events.ingest_all_enabled()

    # 2. 聚合
    svc_agg = service_for("realtime_agg")
    steps["aggregation"] = svc_agg.run_all_active()

    # 3. 指标快照
    svc_metrics = service_for("metric_store")
    steps["metrics"] = svc_metrics.compute_all()

    # 4. 分群
    segments = db.fetchall("SELECT id FROM segments LIMIT 6")
    seg_results = []
    for s in segments:
        r = service_for("segment_engine").run_computation(s["id"])
        seg_results.append(r)
    steps["segments"] = {"processed": len(segments), "results": seg_results}

    # 5. 漏斗
    funnels = db.fetchall("SELECT id FROM funnels LIMIT 3")
    funnel_results = []
    for f in funnels:
        r = service_for("funnel_analyzer").run_computation(f["id"])
        funnel_results.append(r)
    steps["funnels"] = {"processed": len(funnels), "results": funnel_results}

    # 6. 留存
    ret_tasks = db.fetchall("SELECT id FROM retention_tasks LIMIT 3")
    ret_results = []
    for t in ret_tasks:
        r = service_for("retention_analysis").run_computation(t["id"])
        ret_results.append(r)
    steps["retention"] = {"processed": len(ret_tasks), "results": ret_results}

    # 7. 预测
    forecast_tasks = db.fetchall("SELECT id FROM forecast_tasks LIMIT 2")
    fc_results = []
    for t in forecast_tasks:
        r = service_for("forecast_model").run_forecast(t["id"])
        fc_results.append(r)
    steps["forecast"] = {"processed": len(forecast_tasks), "results": fc_results}

    # 8. 报告
    reports = db.fetchall("SELECT id FROM reports LIMIT 2")
    report_results = []
    for rep in reports:
        r = service_for("data_report").generate(rep["id"])
        report_results.append(r)
    steps["reports"] = {"processed": len(reports), "results": report_results}

    return {"ok": True, "pipeline": steps}