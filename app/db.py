from __future__ import annotations
from typing import Dict, Any
import hashlib
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from .config import cfg
from .logger import get_logger
from .models import CustomerRecord

log = get_logger()

class MongoDAO:
    def __init__(self, uri: str, db: str, collection: str):
        self.client = MongoClient(uri, appname="ingestion-worker")
        self.col = self.client[db][collection]
        self._ensure_indexes()

    def _ensure_indexes(self):
        # strong idempotency via deterministic _id
        # or alternatively unique compound index
        self.col.create_index([("customerId", ASCENDING)], name="idx_customerId")

    @staticmethod
    def deterministic_id(payload: Dict[str, Any]) -> str:
        # hash of (customerId|email|createdAt)
        key = f"{payload.get('customerId','')}|{payload.get('email','')}|{payload.get('createdAt','')}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def insert_record(self, record: Dict[str, Any]) -> str:
        # validate already done; convert fields
        doc = record.copy()
        doc["_id"] = self.deterministic_id(doc)
        doc["ingestedAt"] = __import__("datetime").datetime.utcnow()
        try:
            self.col.insert_one(doc)
            return doc["_id"]
        except DuplicateKeyError:
            # Idempotent insert; treat as success but log duplicate
            log.warning("duplicate_insert", _id=doc["_id"], customerId=doc.get("customerId"))
            return doc["_id"]
