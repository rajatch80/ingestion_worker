from __future__ import annotations
from typing import TypedDict, Optional

class ErrorLog(TypedDict, total=False):
    status: str
    customerId: Optional[str]
    reason: str
