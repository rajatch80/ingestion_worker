from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from email_validator import validate_email, EmailNotValidError
from dateutil.parser import isoparse

class CustomerRecord(BaseModel):
    customerId: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    email: str
    createdAt: str

    @field_validator("name")
    @classmethod
    def name_not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("name is blank")
        return v

    @field_validator("email")
    @classmethod
    def email_valid(cls, v: str) -> str:
        try:
            info = validate_email(v, check_deliverability=False)
            return info.normalized
        except EmailNotValidError as e:
            raise ValueError(str(e))

    @field_validator("createdAt")
    @classmethod
    def created_at_iso(cls, v: str) -> str:
        try:
            # Accept ISO8601 including 'Z'
            _ = isoparse(v)
            return v
        except Exception:
            raise ValueError("createdAt is not a valid ISO date string")

    def created_at_dt(self) -> datetime:
        return isoparse(self.createdAt)
