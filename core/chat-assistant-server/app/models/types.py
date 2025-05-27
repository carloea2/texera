from typing import Any, Dict, List, Optional

from openai import BaseModel


# Typed models matching frontend interfaces
class OperatorPredicate(BaseModel):
    operatorID: str
    operatorType: str
    operatorVersion: str
    operatorProperties: Dict[str, Any]
    inputPorts: List[Dict[str, Any]]
    outputPorts: List[Dict[str, Any]]
    dynamicInputPorts: Optional[bool] = None
    dynamicOutputPorts: Optional[bool] = None
    showAdvanced: bool
    isDisabled: Optional[bool] = None
    viewResult: Optional[bool] = None
    markedForReuse: Optional[bool] = None
    customDisplayName: Optional[str] = None


class Point(BaseModel):
    x: float
    y: float


class OperatorAndPosition(BaseModel):
    op: OperatorPredicate
    pos: Point


class OperatorLink(BaseModel):
    linkID: str
    source: Dict[str, Any]
    target: Dict[str, Any]
