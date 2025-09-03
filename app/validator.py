from __future__ import annotations
from typing import Dict, Any, Tuple
from .models import CustomerRecord

def validate_record(raw: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Returns (is_valid, payload_or_error)
    On error, payload is: {status:"error", customerId:..., reason:...}
    """
    try:
        rec = CustomerRecord.model_validate(raw)
        return True, rec.model_dump()
    except Exception as e:
        customer_id = raw.get("customerId", None)
        return False, {"status": "error", "customerId": customer_id, "reason": str(e)}
