from __future__ import annotations

import json
import unittest
from pathlib import Path

import polars as pl

from tests.api_test_case import ApiTestCase


class ApiStructureBindingTests(ApiTestCase):
    def test_structure_scan_binds_api_fields_from_python_code_references(self) -> None:
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

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/code-ref-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])
        binding_patches = [
            patch for patch in bundle["patches"]
            if patch["type"] == "add_binding" and patch["target_id"] in {pricing_score_field["id"], market_field["id"]}
        ]
        self.assertEqual(len(binding_patches), 2)
        self.assertTrue(all("code_reference" in patch["evidence"] for patch in binding_patches))

    def test_structure_scan_binds_api_fields_from_helper_serializer(self) -> None:
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
    return serialize_signal(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/helper-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_from_pydantic_model_dump(self) -> None:
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

@router.get("/api/pydantic-scan-pricing-snapshot", response_model=PricingSnapshot)
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    payload = PricingSnapshot(pricing_score=signal.pricing_score, market=signal.market)
    return payload.model_dump()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/pydantic-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_from_dataclass_asdict_helper(self) -> None:
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

@router.get("/api/dataclass-scan-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return asdict(serialize_signal(signal))
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/dataclass-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_from_service_object_method(self) -> None:
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
        serializer = SnapshotSerializer(signal)
        return serializer.to_dict()

@router.get("/api/service-scan-pricing-snapshot")
def pricing_snapshot(session):
    return SnapshotService(session).build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/service-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_across_imported_service_modules(self) -> None:
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

from backend.services import SnapshotService

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/imported-service-scan-pricing-snapshot")
def pricing_snapshot(session):
    return SnapshotService(session, MarketSignal).build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/imported-service-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_module_alias_import(self) -> None:
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

@router.get("/api/module-alias-scan-pricing-snapshot")
def pricing_snapshot(session):
    return svc.SnapshotService(session, MarketSignal).build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/module-alias-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_package_module_alias_reexport(self) -> None:
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

import backend as backend_pkg

router = APIRouter()
Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))
    pricing_score = Column(Numeric)

@router.get("/api/package-module-alias-scan-pricing-snapshot")
def pricing_snapshot(session):
    return backend_pkg.SnapshotService(session, MarketSignal).build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/package-module-alias-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_imported_factory_return_object(self) -> None:
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

@router.get("/api/factory-object-scan-pricing-snapshot")
def pricing_snapshot(session):
    service = factory_mod.build_service(session, MarketSignal)
    return service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/factory-object-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_async_depends_service(self) -> None:
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

@router.get("/api/async-depends-scan-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    payload = await service.build_snapshot()
    return payload
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/async-depends-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_async_imported_wrapper_chain(self) -> None:
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

@router.get("/api/async-wrapper-scan-pricing-snapshot")
async def pricing_snapshot():
    return await wrapper_mod.build_payload()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/async-wrapper-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_module_alias_depends_helper(self) -> None:
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
        (backend_dir / "pricing.py").write_text(
            """
from fastapi import APIRouter, Depends
import backend.deps as deps_mod

router = APIRouter()

@router.get("/api/module-alias-depends-scan-pricing-snapshot")
async def pricing_snapshot(service = Depends(deps_mod.get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/module-alias-depends-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_package_reexport_depends_helper(self) -> None:
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

@router.get("/api/package-reexport-depends-scan-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/package-reexport-depends-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_nested_depends_chain(self) -> None:
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

@router.get("/api/nested-depends-scan-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/nested-depends-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_payload_merge_wrapper(self) -> None:
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

@router.get("/api/payload-merge-scan-pricing-snapshot")
def pricing_snapshot():
    signal = fetch_signal(MarketSignal)
    return build_payload(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/payload-merge-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_object_method_payload_passthrough(self) -> None:
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

@router.get("/api/object-method-passthrough-scan-pricing-snapshot")
def pricing_snapshot():
    signal = query(MarketSignal).first()
    return build_service().build_snapshot(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/object-method-passthrough-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_dict_union_expression(self) -> None:
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

@router.get("/api/payload-union-expression-scan-pricing-snapshot")
def pricing_snapshot():
    signal = fetch_signal(MarketSignal)
    return build_payload(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/payload-union-expression-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_from_conditional_return_expression(self) -> None:
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

@router.get("/api/conditional-expression-scan-pricing-snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score, "market": signal.market} if signal else {}
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/conditional-expression-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_dict_union_update(self) -> None:
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

@router.get("/api/payload-union-scan-pricing-snapshot")
def pricing_snapshot():
    signal = fetch_signal(MarketSignal)
    return build_payload(signal)
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/payload-union-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_two_hop_package_reexport_depends(self) -> None:
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

@router.get("/api/two-hop-package-reexport-scan-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/two-hop-package-reexport-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_annotated_depends_provider(self) -> None:
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

@router.get("/api/annotated-depends-scan-pricing-snapshot")
async def pricing_snapshot(service: Annotated[SnapshotService, Depends(provide_service)]):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/annotated-depends-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

    def test_structure_scan_binds_api_fields_through_multi_dependency_service_factory(self) -> None:
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

@router.get("/api/multi-dependency-factory-scan-pricing-snapshot")
async def pricing_snapshot(service = Depends(get_service)):
    return await service.build_snapshot()
""".strip(),
            encoding="utf-8",
        )

        response = self.client.post(
            "/api/structure/scan",
            json={
                "role": "scout",
                "scope": "full",
                "root_path": str(self.root),
                "include_internal": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        bundle = response.json()["bundle"]
        observed_nodes = {node["id"]: node for node in bundle["observed"]["nodes"]}
        api_node = next(
            node
            for node in observed_nodes.values()
            if node["kind"] == "contract" and node.get("contract", {}).get("route") == "GET /api/multi-dependency-factory-scan-pricing-snapshot"
        )
        data_node = observed_nodes["data:analytics_market_signals"]
        data_fields = {column["name"]: column["id"] for column in data_node["columns"]}
        pricing_score_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "pricing_score")
        market_field = next(field for field in api_node["contract"]["fields"] if field["name"] == "market")
        self.assertEqual(pricing_score_field["primary_binding"], data_fields["pricing_score"])
        self.assertEqual(market_field["primary_binding"], data_fields["market"])

