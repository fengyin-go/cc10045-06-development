"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class ReviewRowPayload(BaseModel):
    """复核文件中的一行异常解释。"""
    row_no: int | None = None
    metric_name: str = ""
    business_time: str = ""
    explanation: str = ""


class ReviewFileImportPayload(BaseModel):
    """复核文件导入请求：结构化行或直接粘贴 CSV 文本。"""
    file_name: str
    uploader: str = ""
    rows: list[ReviewRowPayload] = Field(default_factory=list)
    csv_text: str | None = None


class MetricSamplingPayload(BaseModel):
    """补采请求：为指标在指定业务时点登记一条采样。"""
    metric_id: int | None = None
    metric_name: str | None = None
    snapshot_value: float = 0.0
    snapshot_time: str
