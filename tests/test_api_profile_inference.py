from __future__ import annotations

import json
import unittest
from pathlib import Path

import polars as pl

from tests.api_test_case import ApiTestCase


class ApiProfileInferenceTests(ApiTestCase):
    def test_project_profile_detects_custom_api_and_ui_code_hints(self) -> None:
        backend_dir = self.root / "backend"
        frontend_dir = self.root / "frontend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "routes.py").write_text(
            """
from fastapi import APIRouter
router = APIRouter()

@router.get("/api/custom-health")
def custom_health():
    return {"ok": True}
""".strip(),
            encoding="utf-8",
        )
        (frontend_dir / "MarketDashboard.tsx").write_text(
            """
export function MarketDashboard() {
  return fetch("/api/custom-health");
}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertTrue(any(hint["route"] == "GET /api/custom-health" for hint in payload["api_contract_hints"]))
        self.assertTrue(
            any(
                hint["component"] == "MarketDashboard" and "/api/custom-health" in hint["api_routes"]
                for hint in payload["ui_contract_hints"]
            )
        )

    def test_project_profile_infers_api_response_fields_from_python_route(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class PricingSnapshot(BaseModel):
    pricing_score: float
    rent_index: float

@router.get("/api/pricing-snapshot", response_model=PricingSnapshot)
def pricing_snapshot():
    payload = {"pricing_score": 91.2, "rent_index": 1.04}
    return payload
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/pricing-snapshot")
        self.assertEqual(hint["detected_from"], "fastapi_ast")
        self.assertEqual(hint["response_model"], "PricingSnapshot")
        self.assertEqual(hint["response_fields"], ["pricing_score", "rent_index"])

    def test_project_profile_infers_api_response_field_sources_from_python_route(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/code-ref-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/code-ref-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_helper_and_aliases(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

def serialize_signal(signal):
    score = signal.pricing_score
    market_name = signal.market
    return {"pricing_score": score, "market": market_name}

@router.get("/api/helper-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    payload = serialize_signal(signal)
    return payload
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/helper-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_piecewise_dict_mutation(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/mutated-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    payload = {}
    payload["pricing_score"] = signal.pricing_score
    payload["market"] = signal.market
    return payload
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/mutated-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_dict_constructor(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/dict-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return dict(pricing_score=signal.pricing_score, market=signal.market)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/dict-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_pydantic_model_dump(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

class PricingSnapshot(BaseModel):
    pricing_score: float
    market: str

@router.get("/api/pydantic-pricing-snapshot", response_model=PricingSnapshot)
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return PricingSnapshot(pricing_score=signal.pricing_score, market=signal.market).model_dump()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/pydantic-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_dataclass_asdict_helper(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "serializers.py").write_text(
            """
from dataclasses import dataclass

@dataclass
class PricingSnapshot:
    pricing_score: float
    market: str

def serialize_signal(signal):
    return PricingSnapshot(pricing_score=signal.pricing_score, market=signal.market)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from dataclasses import asdict

from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

from backend.serializers import serialize_signal

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/dataclass-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return asdict(serialize_signal(signal))
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/dataclass-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_list_serializer(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

def serialize_signal(signal):
    return {"pricing_score": signal.pricing_score, "market": signal.market}

@router.get("/api/list-pricing-snapshot")
def pricing_snapshot(session):
    signals = session.query(MarketSignal).all()
    return [serialize_signal(signal) for signal in signals]
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/list-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_query_helper_chain(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

def fetch_signal(session):
    return session.query(MarketSignal).first()

def build_snapshot(session):
    signal = fetch_signal(session)
    return {"pricing_score": signal.pricing_score, "market": signal.market}

@router.get("/api/helper-chain-pricing-snapshot")
def pricing_snapshot(session):
    return build_snapshot(session)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/helper-chain-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_serializer_object(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

@router.get("/api/object-serializer-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    serializer = SnapshotSerializer(signal)
    return serializer.to_dict()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/object-serializer-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_service_object_method(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

def fetch_signal(session):
    return session.query(MarketSignal).first()

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, session):
        self.session = session

    def fetch_signal(self):
        return fetch_signal(self.session)

    def build_snapshot(self):
        signal = self.fetch_signal()
        return SnapshotSerializer(signal).to_dict()

@router.get("/api/service-pricing-snapshot")
def pricing_snapshot(session):
    service = SnapshotService(session)
    return service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/service-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_across_imported_modules(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(session, MarketSignal):
    return session.query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "serializers.py").write_text(
            """
class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal
from backend.serializers import SnapshotSerializer

class SnapshotService:
    def __init__(self, session, MarketSignal):
        self.session = session
        self.MarketSignal = MarketSignal

    def build_snapshot(self):
        signal = fetch_signal(self.session, self.MarketSignal)
        serializer = SnapshotSerializer(signal)
        return serializer.to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

from backend.services import SnapshotService

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/imported-service-pricing-snapshot")
def pricing_snapshot(session):
    service = SnapshotService(session, MarketSignal)
    return service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/imported-service-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_module_alias_import(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(session, MarketSignal):
    return session.query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "serializers.py").write_text(
            """
class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal
from backend.serializers import SnapshotSerializer

class SnapshotService:
    def __init__(self, session, MarketSignal):
        self.session = session
        self.MarketSignal = MarketSignal

    def build_snapshot(self):
        signal = fetch_signal(self.session, self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

import backend.services as svc

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/module-alias-pricing-snapshot")
def pricing_snapshot(session):
    return svc.SnapshotService(session, MarketSignal).build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/module-alias-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_package_reexport(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(session, MarketSignal):
    return session.query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "serializers.py").write_text(
            """
class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal
from backend.serializers import SnapshotSerializer

class SnapshotService:
    def __init__(self, session, MarketSignal):
        self.session = session
        self.MarketSignal = MarketSignal

    def build_snapshot(self):
        signal = fetch_signal(self.session, self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "__init__.py").write_text(
            """
from backend.services import SnapshotService
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

from backend import SnapshotService

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/package-reexport-pricing-snapshot")
def pricing_snapshot(session):
    return SnapshotService(session, MarketSignal).build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/package-reexport-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_imported_factory_return_object(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(session, MarketSignal):
    return session.query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "serializers.py").write_text(
            """
class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal
from backend.serializers import SnapshotSerializer

class SnapshotService:
    def __init__(self, session, MarketSignal):
        self.session = session
        self.MarketSignal = MarketSignal

    def build_snapshot(self):
        signal = fetch_signal(self.session, self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "factories.py").write_text(
            """
from backend.services import SnapshotService

def build_service(session, MarketSignal):
    return SnapshotService(session, MarketSignal)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

import backend.factories as factory_mod

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/factory-object-pricing-snapshot")
def pricing_snapshot(session):
    return factory_mod.build_service(session, MarketSignal).build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/factory-object-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_async_depends_service(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter, Depends
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

def fetch_signal(MarketSignal):
    return session.query(MarketSignal).first()

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, MarketSignal):
        self.MarketSignal = MarketSignal

    async def build_snapshot(self):
        signal = fetch_signal(self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()

def get_service():
    return SnapshotService(MarketSignal)

@router.get("/api/async-depends-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/async-depends-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_async_imported_wrapper_chain(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "serializers.py").write_text(
            """
class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal
from backend.serializers import SnapshotSerializer

class SnapshotService:
    def __init__(self, MarketSignal):
        self.MarketSignal = MarketSignal

    async def build_snapshot(self):
        signal = fetch_signal(self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "factories.py").write_text(
            """
from backend.services import SnapshotService

def build_service(MarketSignal):
    return SnapshotService(MarketSignal)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "wrappers.py").write_text(
            """
from backend.factories import build_service
from backend.models import MarketSignal

async def build_payload():
    service = build_service(MarketSignal)
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
import backend.wrappers as wrapper_mod

router = APIRouter()

@router.get("/api/async-wrapper-pricing-snapshot")
async def pricing_snapshot():
    return await wrapper_mod.build_payload()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/async-wrapper-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_module_alias_depends_helper(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "serializers.py").write_text(
            """
class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, MarketSignal):
        self.MarketSignal = MarketSignal

    async def build_snapshot(self):
        signal = fetch_signal(self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "deps.py").write_text(
            """
from backend.models import MarketSignal
from backend.services import SnapshotService

def get_service():
    return SnapshotService(MarketSignal)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter, Depends
import backend.deps as deps_mod

router = APIRouter()

@router.get("/api/module-alias-depends-pricing-snapshot")
async def pricing_snapshot(service = Depends(deps_mod.get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/module-alias-depends-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_package_reexport_depends_helper(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, MarketSignal):
        self.MarketSignal = MarketSignal

    async def build_snapshot(self):
        signal = fetch_signal(self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "deps.py").write_text(
            """
from backend.models import MarketSignal
from backend.services import SnapshotService

def get_service():
    return SnapshotService(MarketSignal)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "__init__.py").write_text(
            """
from backend.deps import get_service
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter, Depends
from backend import get_service

router = APIRouter()

@router.get("/api/package-reexport-depends-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/package-reexport-depends-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_nested_depends_chain(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "repository.py").write_text(
            """
from backend.helpers import fetch_signal

class SnapshotRepository:
    def __init__(self, MarketSignal):
        self.MarketSignal = MarketSignal

    def fetch_signal(self):
        return fetch_signal(self.MarketSignal)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, repo):
        self.repo = repo

    async def build_snapshot(self):
        signal = self.repo.fetch_signal()
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "deps.py").write_text(
            """
from fastapi import Depends

from backend.models import MarketSignal
from backend.repository import SnapshotRepository
from backend.services import SnapshotService

def get_repo():
    return SnapshotRepository(MarketSignal)

def get_service(repo = Depends(get_repo)):
    return SnapshotService(repo)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter, Depends
from backend.deps import get_service

router = APIRouter()

@router.get("/api/nested-depends-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/nested-depends-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_payload_merge_wrapper(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()

def build_payload(signal):
    base = {"pricing_score": signal.pricing_score}
    payload = {**base}
    payload.update({"market": signal.market})
    return payload
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter

from backend.helpers import build_payload, fetch_signal
from backend.models import MarketSignal

router = APIRouter()

@router.get("/api/payload-merge-pricing-snapshot")
def pricing_snapshot():
    signal = fetch_signal(MarketSignal)
    return build_payload(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/payload-merge-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_object_method_payload_passthrough(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
class SnapshotFormatter:
    def finalize(self, payload, signal):
        payload["market"] = signal.market
        return payload

class SnapshotService:
    def __init__(self, formatter):
        self.formatter = formatter

    def build_snapshot(self, signal):
        payload = {"pricing_score": signal.pricing_score}
        return self.formatter.finalize(payload, signal)

def build_service():
    return SnapshotService(SnapshotFormatter())
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter

from backend.models import MarketSignal
from backend.services import build_service

router = APIRouter()

@router.get("/api/object-method-passthrough-pricing-snapshot")
def pricing_snapshot():
    signal = query(MarketSignal).first()
    return build_service().build_snapshot(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(
            hint
            for hint in payload["api_contract_hints"]
            if hint["route"] == "GET /api/object-method-passthrough-pricing-snapshot"
        )
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_dict_union_expression(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()

def build_payload(signal):
    base = {"pricing_score": signal.pricing_score}
    return base | {"market": signal.market}
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter

from backend.helpers import build_payload, fetch_signal
from backend.models import MarketSignal

router = APIRouter()

@router.get("/api/payload-union-expression-pricing-snapshot")
def pricing_snapshot():
    signal = fetch_signal(MarketSignal)
    return build_payload(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(
            hint
            for hint in payload["api_contract_hints"]
            if hint["route"] == "GET /api/payload-union-expression-pricing-snapshot"
        )
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_from_conditional_return_expression(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/conditional-expression-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market} if signal else {}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(
            hint
            for hint in payload["api_contract_hints"]
            if hint["route"] == "GET /api/conditional-expression-pricing-snapshot"
        )
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_dict_union_update(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()

def build_payload(signal):
    payload = {"pricing_score": signal.pricing_score}
    payload |= {"market": signal.market}
    return payload
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter

from backend.helpers import build_payload, fetch_signal
from backend.models import MarketSignal

router = APIRouter()

@router.get("/api/payload-union-pricing-snapshot")
def pricing_snapshot():
    signal = fetch_signal(MarketSignal)
    return build_payload(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/payload-union-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_two_hop_package_reexport_depends(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, MarketSignal):
        self.MarketSignal = MarketSignal

    async def build_snapshot(self):
        signal = fetch_signal(self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "deps.py").write_text(
            """
from backend.models import MarketSignal
from backend.services import SnapshotService

def get_service():
    return SnapshotService(MarketSignal)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "api_exports.py").write_text(
            """
from backend.deps import get_service
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "__init__.py").write_text(
            """
from backend.api_exports import get_service
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter, Depends
from backend import get_service

router = APIRouter()

@router.get("/api/two-hop-package-reexport-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/two-hop-package-reexport-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_annotated_depends_provider(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
from backend.helpers import fetch_signal

class SnapshotSerializer:
    def __init__(self, signal):
        self.signal = signal

    def to_dict(self):
        return {"pricing_score": self.signal.pricing_score, "market": self.signal.market}

class SnapshotService:
    def __init__(self, MarketSignal):
        self.MarketSignal = MarketSignal

    async def build_snapshot(self):
        signal = fetch_signal(self.MarketSignal)
        return SnapshotSerializer(signal).to_dict()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "deps.py").write_text(
            """
from backend.models import MarketSignal
from backend.services import SnapshotService

def get_service():
    return SnapshotService(MarketSignal)

def provide_service():
    return get_service()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from typing import Annotated

from fastapi import APIRouter, Depends
from backend.deps import provide_service
from backend.services import SnapshotService

router = APIRouter()

@router.get("/api/annotated-depends-pricing-snapshot")
async def pricing_snapshot(service: Annotated[SnapshotService, Depends(provide_service)]):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/annotated-depends-pricing-snapshot")
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_api_response_field_sources_through_multi_dependency_service_factory(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "helpers.py").write_text(
            """
def fetch_signal(MarketSignal):
    return query(MarketSignal).first()
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "repository.py").write_text(
            """
from backend.helpers import fetch_signal

class SnapshotRepository:
    def __init__(self, MarketSignal):
        self.MarketSignal = MarketSignal

    def fetch_signal(self):
        return fetch_signal(self.MarketSignal)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "formatters.py").write_text(
            """
class SnapshotFormatter:
    def render(self, signal):
        return {"pricing_score": signal.pricing_score, "market": signal.market}
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "services.py").write_text(
            """
class SnapshotService:
    def __init__(self, repo, formatter):
        self.repo = repo
        self.formatter = formatter

    async def build_snapshot(self):
        signal = self.repo.fetch_signal()
        return self.formatter.render(signal)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "deps.py").write_text(
            """
from fastapi import Depends

from backend.formatters import SnapshotFormatter
from backend.models import MarketSignal
from backend.repository import SnapshotRepository
from backend.services import SnapshotService

def get_repo():
    return SnapshotRepository(MarketSignal)

def get_formatter():
    return SnapshotFormatter()

def get_service(
    repo = Depends(get_repo),
    formatter = Depends(get_formatter),
):
    return SnapshotService(repo, formatter)
""".strip(),
            encoding="utf-8",
        )
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter, Depends
from backend.deps import get_service

router = APIRouter()

@router.get("/api/multi-dependency-factory-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(
            hint for hint in payload["api_contract_hints"] if hint["route"] == "GET /api/multi-dependency-factory-pricing-snapshot"
        )
        self.assertEqual(
            hint["response_field_sources"],
            [
                {"name": "market", "source_fields": [{"relation": "analytics.market_signals", "column": "market"}]},
                {"name": "pricing_score", "source_fields": [{"relation": "analytics.market_signals", "column": "pricing_score"}]},
            ],
        )

    def test_project_profile_infers_sql_structure_hints(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_signals.sql").write_text(
            """
create table analytics.market_inputs (
    market text,
    median_home_price numeric,
    rent_index numeric
);

create materialized view analytics.market_signals as
select
    inputs.market as market,
    inputs.median_home_price as median_home_price,
    inputs.rent_index as rent_index,
    inputs.median_home_price / nullif(inputs.rent_index, 0) as pricing_score
from analytics.market_inputs as inputs;
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertGreaterEqual(payload["summary"]["sql_structure_hints"], 2)
        table_hint = next(
            hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_inputs"
        )
        view_hint = next(
            hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_signals"
        )
        self.assertEqual(table_hint["object_type"], "table")
        self.assertEqual(
            [field["name"] for field in table_hint["fields"]],
            ["market", "median_home_price", "rent_index"],
        )
        self.assertEqual(view_hint["object_type"], "materialized_view")
        self.assertEqual(view_hint["upstream_relations"], ["analytics.market_inputs"])
        self.assertTrue(any(field["name"] == "pricing_score" for field in view_hint["fields"]))

    def test_project_profile_infers_orm_structure_hints(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Integer, Numeric, String, ForeignKey
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market: Mapped[str] = mapped_column(String(32))
    pricing_score: Mapped[float] = mapped_column(Numeric)

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = {"schema": "analytics"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_signal_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_signals.id"))
    signal = relationship("MarketSignal")
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        self.assertGreaterEqual(payload["summary"]["orm_structure_hints"], 2)
        market_signal_hint = next(
            hint for hint in payload["orm_structure_hints"] if hint["relation"] == "analytics.market_signals"
        )
        snapshot_hint = next(
            hint for hint in payload["orm_structure_hints"] if hint["relation"] == "analytics.market_snapshots"
        )
        self.assertEqual(market_signal_hint["detected_from"], "sqlalchemy_model")
        self.assertEqual(
            [field["name"] for field in market_signal_hint["fields"]],
            ["id", "market", "pricing_score"],
        )
        self.assertEqual(
            [field["data_type"] for field in market_signal_hint["fields"]],
            ["integer", "string", "float"],
        )
        market_signal_id_field = next(field for field in market_signal_hint["fields"] if field["name"] == "id")
        self.assertTrue(market_signal_id_field["primary_key"])
        self.assertEqual(
            [field["name"] for field in snapshot_hint["fields"]],
            ["id", "market_signal_id"],
        )
        self.assertEqual(
            [field["data_type"] for field in snapshot_hint["fields"]],
            ["integer", "integer"],
        )
        snapshot_id_field = next(field for field in snapshot_hint["fields"] if field["name"] == "id")
        self.assertTrue(snapshot_id_field["primary_key"])
        self.assertEqual(snapshot_hint["upstream_relations"], ["analytics.market_signals"])
        snapshot_fk = next(field for field in snapshot_hint["fields"] if field["name"] == "market_signal_id")
        self.assertEqual(snapshot_fk["foreign_key"], "analytics.market_signals.id")
        self.assertEqual(
            snapshot_fk["source_fields"],
            [{"relation": "analytics.market_signals", "column": "id"}],
        )

    def test_project_profile_infers_ui_used_fields_from_fetch_json_flow(self) -> None:
        frontend_dir = self.root / "frontend"
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (frontend_dir / "MarketDashboard.tsx").write_text(
            """
export async function MarketDashboard() {
  const snapshot = await fetch("/api/markets/snapshot").then((response) => response.json());
  const { medianHomePrice } = snapshot;
  return `${snapshot.pricingScore} ${snapshot["rentIndex"]} ${medianHomePrice}`;
}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        hint = next(hint for hint in payload["ui_contract_hints"] if hint["component"] == "MarketDashboard")
        self.assertEqual(hint["api_routes"], ["/api/markets/snapshot"])
        self.assertEqual(hint["used_fields"], ["medianHomePrice", "pricingScore", "rentIndex"])
        self.assertEqual(
            hint["route_field_hints"]["/api/markets/snapshot"],
            ["medianHomePrice", "pricingScore", "rentIndex"],
        )

    def test_project_profile_infers_ui_routes_from_template_literals_and_simple_builders(self) -> None:
        frontend_dir = self.root / "frontend"
        frontend_dir.mkdir(parents=True, exist_ok=True)
        (frontend_dir / "MarketMapEmbed.tsx").write_text(
            """
const endpoint = "/api/maps/market-embed";
const aliasEndpoint = endpoint;
const buildSearchRoute = (slug) => `/api/search/${slug}`;

export async function MarketMapEmbed({ marketSlug }) {
  const response = await fetch(`${config.apiUrl}${aliasEndpoint}`, {
    method: "GET",
  });
  const payload = await response.json();
  await fetch(buildSearchRoute(marketSlug));
  return `${payload.pricingScore}`;
}
""".strip(),
            encoding="utf-8",
        )
        (frontend_dir / "DynamicOnly.tsx").write_text(
            """
export async function DynamicOnly({ endpoint }) {
  return fetch(endpoint);
}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        hint = next(hint for hint in payload["ui_contract_hints"] if hint["component"] == "MarketMapEmbed")
        self.assertEqual(hint["api_routes"], ["/api/maps/market-embed", "/api/search/${slug}"])
        self.assertEqual(hint["route_field_hints"]["/api/maps/market-embed"], ["pricingScore"])
        self.assertFalse(any(hint["component"] == "DynamicOnly" for hint in payload["ui_contract_hints"]))

    def test_project_profile_builds_collision_safe_api_hint_ids_and_normalizes_empty_paths(self) -> None:
        backend_dir = self.root / "backend" / "api" / "features" / "green"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "routes.py").write_text(
            """
from fastapi import APIRouter

router = APIRouter()

@router.get("/buildings/{building_slug}/green")
def building_green():
    return {"scope": "building"}

@router.get("/boroughs/{borough_slug}/green")
def borough_green():
    return {"scope": "borough"}

@router.get("/areas/{area_kind}/{area_slug}/green")
def area_green():
    return {"scope": "area"}

@router.post("")
def create_green():
    return {"ok": True}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        green_hints = [
            hint for hint in payload["api_contract_hints"]
            if hint["file"].endswith("backend/api/features/green/routes.py")
        ]
        self.assertEqual(len({hint["id"] for hint in green_hints}), len(green_hints))
        self.assertTrue(all(hint["id"].startswith("api:") for hint in green_hints))
        self.assertIn("POST /", {hint["route"] for hint in green_hints})
