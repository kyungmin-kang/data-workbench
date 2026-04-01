from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from workbench.app import app
from tests.workbench_test_support import WorkbenchTempRootMixin


class SqlOrmCoverageTests(WorkbenchTempRootMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.setUpWorkbenchRoot()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.tearDownWorkbenchRoot()

    def test_project_profile_infers_sql_cte_join_lineage_and_ambiguity(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_signals.sql").write_text(
            """
create table analytics.market_inputs (
    id integer,
    market text,
    weight_id integer,
    median_home_price numeric
);

create table analytics.market_weights (
    id integer,
    weight numeric,
    label text
);

create view analytics.market_signals_enriched as
with weighted_inputs as (
    select
        inputs.market as market,
        inputs.median_home_price * weights.weight as weighted_price
    from analytics.market_inputs as inputs
    join analytics.market_weights as weights on inputs.weight_id = weights.id
)
select
    weighted_inputs.market as market,
    weighted_inputs.weighted_price as pricing_score
from weighted_inputs;

create view analytics.market_join_ids as
select
    id as joined_id
from analytics.market_inputs as inputs
join analytics.market_weights as weights on inputs.weight_id = weights.id;
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        enriched_hint = next(
            hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_signals_enriched"
        )
        self.assertEqual(
            enriched_hint["upstream_relations"],
            ["analytics.market_inputs", "analytics.market_weights"],
        )
        pricing_score_field = next(field for field in enriched_hint["fields"] if field["name"] == "pricing_score")
        self.assertEqual(
            pricing_score_field["source_fields"],
            [
                {"relation": "analytics.market_inputs", "column": "median_home_price"},
                {"relation": "analytics.market_weights", "column": "weight"},
            ],
        )

        ambiguous_hint = next(
            hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_join_ids"
        )
        joined_id_field = next(field for field in ambiguous_hint["fields"] if field["name"] == "joined_id")
        self.assertEqual(joined_id_field["source_fields"], [])
        self.assertEqual(joined_id_field["confidence"], "low")
        self.assertTrue(joined_id_field["unresolved_sources"])

    def test_structure_scan_observes_sql_cte_join_lineage(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_signals.sql").write_text(
            """
create table analytics.market_inputs (
    market text,
    weight_id integer,
    median_home_price numeric
);

create table analytics.market_weights (
    id integer,
    weight numeric
);

create view analytics.market_signals_enriched as
with weighted_inputs as (
    select
        inputs.market as market,
        inputs.median_home_price * weights.weight as weighted_price
    from analytics.market_inputs as inputs
    join analytics.market_weights as weights on inputs.weight_id = weights.id
)
select
    weighted_inputs.market as market,
    weighted_inputs.weighted_price as pricing_score
from weighted_inputs;
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
        transform_node = observed_nodes["compute:transform.analytics_market_signals_enriched"]
        self.assertEqual(
            transform_node["compute"]["inputs"],
            ["data:analytics_market_inputs", "data:analytics_market_weights"],
        )

        field_owners = {
            column["id"]: (node["id"], column["name"])
            for node in observed_nodes.values()
            for column in node.get("columns", [])
        }
        pricing_score_column = next(column for column in transform_node["columns"] if column["name"] == "pricing_score")
        self.assertEqual(
            sorted(field_owners[lineage["field_id"]] for lineage in pricing_score_column["lineage_inputs"]),
            [
                ("data:analytics_market_inputs", "median_home_price"),
                ("data:analytics_market_weights", "weight"),
            ],
        )

    def test_project_profile_infers_sql_union_subquery_window_and_quoted_identifiers(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_rollup.sql").write_text(
            """
create table "analytics"."market_inputs" (
    "market" text,
    "median_home_price" numeric
);

create table analytics.market_archive (
    market text,
    median_home_price numeric
);

create view analytics.market_rollup as
select
    combined.market as market,
    combined.median_home_price as pricing_score,
    row_number() over (
        partition by combined.market
        order by combined.median_home_price desc
    ) as market_rank
from (
    select
        "market" as market,
        "median_home_price" as median_home_price
    from "analytics"."market_inputs"
    union all
    select
        archive.market as market,
        archive.median_home_price as median_home_price
    from analytics.market_archive as archive
) as combined;
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        rollup_hint = next(
            hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_rollup"
        )
        self.assertEqual(
            rollup_hint["upstream_relations"],
            ["analytics.market_archive", "analytics.market_inputs"],
        )
        pricing_score_field = next(field for field in rollup_hint["fields"] if field["name"] == "pricing_score")
        self.assertEqual(
            pricing_score_field["source_fields"],
            [
                {"relation": "analytics.market_inputs", "column": "median_home_price"},
                {"relation": "analytics.market_archive", "column": "median_home_price"},
            ],
        )
        market_rank_field = next(field for field in rollup_hint["fields"] if field["name"] == "market_rank")
        self.assertEqual(market_rank_field["confidence"], "medium")
        self.assertEqual(
            market_rank_field["source_fields"],
            [
                {"relation": "analytics.market_inputs", "column": "market"},
                {"relation": "analytics.market_archive", "column": "market"},
                {"relation": "analytics.market_inputs", "column": "median_home_price"},
                {"relation": "analytics.market_archive", "column": "median_home_price"},
            ],
        )

    def test_project_profile_infers_sql_cte_column_aliases_without_literal_false_lineage(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_flags.sql").write_text(
            """
create table analytics.market_inputs (
    market text,
    region text,
    median_home_price numeric
);

create view analytics.market_flags as
with staged ("region_name", "market_label", "status_text", "score") as (
    select
        region,
        market,
        'active',
        median_home_price * 1.1
    from analytics.market_inputs
)
select
    staged.region_name,
    staged.market_label,
    staged.status_text,
    staged.score
from staged;
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]
        flags_hint = next(hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_flags")

        region_name_field = next(field for field in flags_hint["fields"] if field["name"] == "region_name")
        self.assertEqual(
            region_name_field["source_fields"],
            [{"relation": "analytics.market_inputs", "column": "region"}],
        )
        market_label_field = next(field for field in flags_hint["fields"] if field["name"] == "market_label")
        self.assertEqual(
            market_label_field["source_fields"],
            [{"relation": "analytics.market_inputs", "column": "market"}],
        )
        status_text_field = next(field for field in flags_hint["fields"] if field["name"] == "status_text")
        self.assertEqual(status_text_field["source_fields"], [])
        self.assertEqual(status_text_field["confidence"], "medium")
        self.assertFalse(status_text_field.get("unresolved_sources"))
        score_field = next(field for field in flags_hint["fields"] if field["name"] == "score")
        self.assertEqual(
            score_field["source_fields"],
            [{"relation": "analytics.market_inputs", "column": "median_home_price"}],
        )
        self.assertEqual(score_field["confidence"], "medium")

    def test_project_profile_marks_sql_orm_schema_match_evidence(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "market_signals.sql").write_text(
            """
create table analytics.market_signals (
    id integer,
    market text,
    pricing_score numeric
);
""".strip(),
            encoding="utf-8",
        )
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

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        sql_hint = next(hint for hint in payload["sql_structure_hints"] if hint["relation"] == "analytics.market_signals")
        orm_hint = next(hint for hint in payload["orm_structure_hints"] if hint["relation"] == "analytics.market_signals")
        self.assertIn("schema_match", sql_hint["evidence"])
        self.assertIn("schema_match", orm_hint["evidence"])
        sql_market_field = next(field for field in sql_hint["fields"] if field["name"] == "market")
        orm_market_field = next(field for field in orm_hint["fields"] if field["name"] == "market")
        self.assertIn("schema_match", sql_market_field["evidence"])
        self.assertIn("schema_match", orm_market_field["evidence"])

    def test_project_profile_infers_orm_associations_and_query_projections(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, ForeignKey, Integer, String, Table, select
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

user_groups = Table(
    "user_groups",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("analytics.users.id"), primary_key=True),
    Column("group_id", Integer, ForeignKey("analytics.groups.id"), primary_key=True),
    schema="analytics",
)

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    groups = relationship("Group", secondary=user_groups)

class Group(Base):
    __tablename__ = "groups"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    users = relationship("User", secondary=user_groups)

def list_memberships(session):
    rows = session.execute(
        select(
            User.id.label("user_id"),
            Group.name.label("group_name"),
        ).join(user_groups, user_groups.c.user_id == User.id).join(Group, user_groups.c.group_id == Group.id)
    ).mappings().all()
    return rows
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        association_hint = next(
            hint for hint in payload["orm_structure_hints"] if hint["relation"] == "analytics.user_groups"
        )
        self.assertEqual(
            [field["name"] for field in association_hint["fields"]],
            ["user_id", "group_id"],
        )
        user_group_field = next(field for field in association_hint["fields"] if field["name"] == "user_id")
        self.assertEqual(
            user_group_field["source_fields"],
            [{"relation": "analytics.users", "column": "id"}],
        )

        user_hint = next(hint for hint in payload["orm_structure_hints"] if hint["relation"] == "analytics.users")
        self.assertEqual(
            user_hint["relationships"][0]["secondary_relation"],
            "analytics.user_groups",
        )

        query_hint = next(
            hint
            for hint in payload["orm_structure_hints"]
            if hint["object_type"] == "query_projection" and hint["query_context"]["function"] == "list_memberships"
        )
        self.assertEqual(
            [field["name"] for field in query_hint["fields"]],
            ["user_id", "group_name"],
        )
        group_name_field = next(field for field in query_hint["fields"] if field["name"] == "group_name")
        self.assertEqual(
            group_name_field["source_fields"],
            [{"relation": "analytics.groups", "column": "name"}],
        )
        self.assertEqual(
            query_hint["upstream_relations"],
            ["analytics.groups", "analytics.user_groups", "analytics.users"],
        )

    def test_project_profile_infers_orm_alias_loader_and_hybrid_expression_hints(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, ForeignKey, Integer, String, Table, select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import aliased, declarative_base, joinedload, load_only, relationship

Base = declarative_base()

user_groups = Table(
    "user_groups",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("analytics.users.id"), primary_key=True),
    Column("group_id", Integer, ForeignKey("analytics.groups.id"), primary_key=True),
    schema="analytics",
)

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    groups = relationship("Group", secondary=user_groups)

    @hybrid_property
    def display_name(self):
        return self.name

    @display_name.expression
    def display_name(cls):
        return cls.name

class Group(Base):
    __tablename__ = "groups"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    users = relationship("User", secondary=user_groups)

def load_users(session):
    user_alias = aliased(User)
    rows = session.scalars(
        select(user_alias).options(
            load_only(user_alias.id, user_alias.name),
            joinedload(User.groups),
        )
    ).all()
    return rows

def load_user_labels(session):
    user_alias = aliased(User)
    labels = session.execute(
        select(
            user_alias.id.label("user_id"),
            user_alias.display_name.label("display_name"),
        )
    ).mappings().all()
    return labels
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        user_hint = next(hint for hint in payload["orm_structure_hints"] if hint["relation"] == "analytics.users")
        computed_display_name = next(field for field in user_hint["computed_fields"] if field["name"] == "display_name")
        self.assertEqual(
            computed_display_name["source_fields"],
            [{"relation": "analytics.users", "column": "name"}],
        )

        load_users_hint = next(
            hint
            for hint in payload["orm_structure_hints"]
            if hint["object_type"] == "query_projection" and hint["query_context"]["function"] == "load_users"
        )
        self.assertEqual(
            [field["name"] for field in load_users_hint["fields"]],
            ["id", "name"],
        )
        self.assertEqual(
            load_users_hint["upstream_relations"],
            ["analytics.groups", "analytics.users"],
        )

        load_user_labels_hint = next(
            hint
            for hint in payload["orm_structure_hints"]
            if hint["object_type"] == "query_projection" and hint["query_context"]["function"] == "load_user_labels"
        )
        display_name_field = next(field for field in load_user_labels_hint["fields"] if field["name"] == "display_name")
        self.assertEqual(
            display_name_field["source_fields"],
            [{"relation": "analytics.users", "column": "name"}],
        )
        user_id_field = next(field for field in load_user_labels_hint["fields"] if field["name"] == "user_id")
        self.assertEqual(
            user_id_field["source_fields"],
            [{"relation": "analytics.users", "column": "id"}],
        )

    def test_project_profile_infers_orm_selected_columns_from_query_variables(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, String, select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))

    @hybrid_property
    def display_name(self):
        return self.name

    @display_name.expression
    def display_name(cls):
        return cls.name

def load_user_pairs(session):
    query = session.query(User).with_entities(
        User.id.label("user_id"),
        User.name.label("user_name"),
    )
    rows = query.all()
    return rows

def load_user_labels(session):
    stmt = select(
        User.id.label("user_id"),
        User.display_name.label("display_name"),
    )
    rows = session.execute(stmt).mappings().all()
    return rows
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        load_user_pairs_hint = next(
            hint
            for hint in payload["orm_structure_hints"]
            if hint["object_type"] == "query_projection"
            and hint["query_context"]["function"] == "load_user_pairs"
            and hint["query_context"]["variable"] == "rows"
        )
        self.assertEqual(
            [field["name"] for field in load_user_pairs_hint["fields"]],
            ["user_id", "user_name"],
        )
        self.assertEqual(
            load_user_pairs_hint["upstream_relations"],
            ["analytics.users"],
        )

        load_user_labels_hint = next(
            hint
            for hint in payload["orm_structure_hints"]
            if hint["object_type"] == "query_projection"
            and hint["query_context"]["function"] == "load_user_labels"
            and hint["query_context"]["variable"] == "rows"
        )
        display_name_field = next(field for field in load_user_labels_hint["fields"] if field["name"] == "display_name")
        self.assertEqual(
            display_name_field["source_fields"],
            [{"relation": "analytics.users", "column": "name"}],
        )
        self.assertTrue(load_user_labels_hint["query_context"]["returns_mapping"])

    def test_structure_scan_observes_association_table_foreign_keys(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

user_groups = Table(
    "user_groups",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("analytics.users.id"), primary_key=True),
    Column("group_id", Integer, ForeignKey("analytics.groups.id"), primary_key=True),
    schema="analytics",
)

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    groups = relationship("Group", secondary=user_groups)

class Group(Base):
    __tablename__ = "groups"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    users = relationship("User", secondary=user_groups)
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
        association_node = observed_nodes["data:analytics_user_groups"]
        self.assertEqual(
            [column["name"] for column in association_node["columns"]],
            ["group_id", "user_id"],
        )
        field_owners = {
            column["id"]: (node["id"], column["name"])
            for node in observed_nodes.values()
            for column in node.get("columns", [])
        }
        user_id_column = next(column for column in association_node["columns"] if column["name"] == "user_id")
        group_id_column = next(column for column in association_node["columns"] if column["name"] == "group_id")
        self.assertEqual(
            [field_owners[lineage["field_id"]] for lineage in user_id_column["lineage_inputs"]],
            [("data:analytics_users", "id")],
        )
        self.assertEqual(
            [field_owners[lineage["field_id"]] for lineage in group_id_column["lineage_inputs"]],
            [("data:analytics_groups", "id")],
        )

    def test_structure_scan_observes_orm_query_projection_nodes_with_stable_field_ids(self) -> None:
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, ForeignKey, Integer, String, Table, select
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

user_groups = Table(
    "user_groups",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("analytics.users.id"), primary_key=True),
    Column("group_id", Integer, ForeignKey("analytics.groups.id"), primary_key=True),
    schema="analytics",
)

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    groups = relationship("Group", secondary=user_groups)

class Group(Base):
    __tablename__ = "groups"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))

def list_memberships(session):
    rows = session.execute(
        select(
            User.id.label("user_id"),
            Group.name.label("group_name"),
        ).join(user_groups, user_groups.c.user_id == User.id).join(Group, user_groups.c.group_id == Group.id)
    ).mappings().all()
    return rows
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
        query_data_node = observed_nodes["data:query_backend_models_list_memberships_rows"]
        query_compute_node = observed_nodes["compute:transform.query_backend_models_list_memberships_rows"]
        self.assertEqual(query_data_node["extension_type"], "feature_set")
        self.assertEqual(query_data_node["data"]["persistence"], "transient")
        self.assertEqual(
            [column["id"] for column in query_data_node["columns"]],
            [
                "field.query_backend_models_list_memberships_rows.group_name",
                "field.query_backend_models_list_memberships_rows.user_id",
            ],
        )
        self.assertEqual(query_compute_node["compute"]["runtime"], "orm")
        self.assertEqual(
            query_compute_node["compute"]["inputs"],
            ["data:analytics_groups", "data:analytics_user_groups", "data:analytics_users"],
        )
        self.assertEqual(query_compute_node["compute"]["outputs"], [query_data_node["id"]])

        field_owners = {
            column["id"]: (node["id"], column["name"])
            for node in observed_nodes.values()
            for column in node.get("columns", [])
        }
        group_name_compute_column = next(column for column in query_compute_node["columns"] if column["name"] == "group_name")
        self.assertEqual(
            [field_owners[lineage["field_id"]] for lineage in group_name_compute_column["lineage_inputs"]],
            [("data:analytics_groups", "name")],
        )
        group_name_data_column = next(column for column in query_data_node["columns"] if column["name"] == "group_name")
        self.assertEqual(
            [field_owners[lineage["field_id"]] for lineage in group_name_data_column["lineage_inputs"]],
            [("data:analytics_groups", "name")],
        )

    def test_structure_scan_is_idempotent_for_mixed_sql_and_orm_inputs(self) -> None:
        sql_dir = self.root / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "signals.sql").write_text(
            """
create table analytics.market_inputs (
    market text,
    median_home_price numeric
);

create table analytics.market_archive (
    market text,
    median_home_price numeric
);

create view analytics.market_rollup as
select
    combined.market as market,
    combined.median_home_price as pricing_score
from (
    select market, median_home_price from analytics.market_inputs
    union all
    select market, median_home_price from analytics.market_archive
) as combined;
""".strip(),
            encoding="utf-8",
        )
        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, String, select
from sqlalchemy.orm import aliased, declarative_base, load_only

Base = declarative_base()

class MarketSignal(Base):
    __tablename__ = "market_signals"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    market = Column(String(32))

def load_signals(session):
    signal_alias = aliased(MarketSignal)
    rows = session.scalars(
        select(signal_alias).options(load_only(signal_alias.id, signal_alias.market))
    ).all()
    return rows
""".strip(),
            encoding="utf-8",
        )

        payload = {
            "role": "scout",
            "scope": "full",
            "root_path": str(self.root),
            "include_internal": False,
        }
        first = self.client.post("/api/structure/scan", json=payload)
        second = self.client.post("/api/structure/scan", json=payload)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        first_observed_nodes = {node["id"]: node for node in first.json()["bundle"]["observed"]["nodes"]}
        self.assertIn("data:query_backend_models_load_signals_rows", first_observed_nodes)
        self.assertIn("compute:transform.query_backend_models_load_signals_rows", first_observed_nodes)
        self.assertEqual(first.json()["bundle"]["bundle_id"], second.json()["bundle"]["bundle_id"])
        self.assertEqual(first.json()["bundle"], second.json()["bundle"])

    def test_structure_scan_observes_imported_repo_orm_query_projection_with_module_aliases(self) -> None:
        backend_dir = self.root / "backend"
        reports_dir = backend_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "__init__.py").write_text("", encoding="utf-8")
        (reports_dir / "__init__.py").write_text("", encoding="utf-8")
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

user_groups = Table(
    "user_groups",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("analytics.users.id"), primary_key=True),
    Column("group_id", Integer, ForeignKey("analytics.groups.id"), primary_key=True),
    schema="analytics",
)

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    groups = relationship("Group", secondary=user_groups)

class Group(Base):
    __tablename__ = "groups"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
""".strip(),
            encoding="utf-8",
        )
        (reports_dir / "queries.py").write_text(
            """
import backend.models as models
from sqlalchemy import select
from sqlalchemy.orm import joinedload, load_only

def list_memberships(session):
    rows = session.execute(
        select(
            models.User.id.label("user_id"),
            models.Group.name.label("group_name"),
        )
        .join(models.user_groups, models.user_groups.c.user_id == models.User.id)
        .join(models.Group, models.user_groups.c.group_id == models.Group.id)
    ).mappings().all()
    return rows

def load_users(session):
    rows = session.query(models.User).options(
        load_only(models.User.id, models.User.name),
        joinedload(models.User.groups),
    ).all()
    return rows
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
        query_data_node = observed_nodes["data:query_backend_reports_queries_list_memberships_rows"]
        query_compute_node = observed_nodes["compute:transform.query_backend_reports_queries_list_memberships_rows"]
        self.assertEqual(
            [column["id"] for column in query_data_node["columns"]],
            [
                "field.query_backend_reports_queries_list_memberships_rows.group_name",
                "field.query_backend_reports_queries_list_memberships_rows.user_id",
            ],
        )
        self.assertEqual(
            query_compute_node["compute"]["inputs"],
            ["data:analytics_groups", "data:analytics_user_groups", "data:analytics_users"],
        )
        field_owners = {
            column["id"]: (node["id"], column["name"])
            for node in observed_nodes.values()
            for column in node.get("columns", [])
        }
        group_name_column = next(column for column in query_compute_node["columns"] if column["name"] == "group_name")
        self.assertEqual(
            [field_owners[lineage["field_id"]] for lineage in group_name_column["lineage_inputs"]],
            [("data:analytics_groups", "name")],
        )
        load_users_node = observed_nodes["data:query_backend_reports_queries_load_users_rows"]
        self.assertEqual([column["name"] for column in load_users_node["columns"]], ["id", "name"])

    def test_project_profile_surfaces_unresolved_raw_sql_orm_projection_fields(self) -> None:
        backend_dir = self.root / "backend"
        queries_dir = backend_dir / "queries"
        queries_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "__init__.py").write_text("", encoding="utf-8")
        (queries_dir / "__init__.py").write_text("", encoding="utf-8")
        (backend_dir / "models.py").write_text(
            """
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "analytics"}

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
""".strip(),
            encoding="utf-8",
        )
        (queries_dir / "external_scores.py").write_text(
            """
from sqlalchemy import literal_column, select

from backend.models import User

def load_external_scores(session):
    rows = session.execute(
        select(
            User.id.label("user_id"),
            literal_column("external_scores.score").label("external_score"),
        )
    ).mappings().all()
    return rows
""".strip(),
            encoding="utf-8",
        )

        response = self.client.get("/api/project/profile?include_internal=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()["project_profile"]

        query_hint = next(
            hint
            for hint in payload["orm_structure_hints"]
            if hint["object_type"] == "query_projection"
            and hint["query_context"]["function"] == "load_external_scores"
        )
        self.assertEqual(
            [field["name"] for field in query_hint["fields"]],
            ["user_id", "external_score"],
        )
        external_score_field = next(field for field in query_hint["fields"] if field["name"] == "external_score")
        self.assertEqual(external_score_field["source_fields"], [])
        self.assertEqual(external_score_field["confidence"], "low")
        self.assertEqual(
            external_score_field["unresolved_sources"],
            [{"token": "external_scores.score", "reason": "raw_sql_projection"}],
        )
        self.assertEqual(query_hint["confidence"], "low")
        self.assertEqual(query_hint["upstream_relations"], ["analytics.users"])

    def test_structure_scan_reports_downstream_breakage_for_imported_orm_api_bindings(self) -> None:
        import_response = self.client.post(
            "/api/structure/import",
            json={
                "spec": {
                    "metadata": {"name": "SQL ORM Breakage"},
                    "nodes": [
                        {
                            "id": "data:analytics_market_signals",
                            "kind": "data",
                            "extension_type": "table",
                            "label": "Market Signals",
                            "tags": ["sql_relation:analytics.market_signals"],
                            "columns": [
                                {
                                    "id": "field.analytics_market_signals.market",
                                    "name": "market",
                                    "data_type": "string",
                                },
                                {
                                    "id": "field.analytics_market_signals.pricing_score",
                                    "name": "pricing_score",
                                    "data_type": "float",
                                },
                            ],
                            "source": {},
                            "data": {"persistence": "hot", "persisted": True},
                            "compute": {},
                            "contract": {},
                            "state": "confirmed",
                            "verification_state": "confirmed",
                            "confidence": "high",
                        },
                        {
                            "id": "contract:api.market_snapshot",
                            "kind": "contract",
                            "extension_type": "api",
                            "label": "Market Snapshot",
                            "columns": [],
                            "source": {},
                            "data": {},
                            "compute": {},
                            "contract": {
                                "route": "GET /api/markets/snapshot",
                                "fields": [
                                    {
                                        "id": "field.api_market_snapshot.pricing_score",
                                        "name": "pricing_score",
                                        "required": True,
                                        "primary_binding": "field.analytics_market_signals.market",
                                        "alternatives": [],
                                        "sources": [
                                            {
                                                "node_id": "data:analytics_market_signals",
                                                "column": "market",
                                            }
                                        ],
                                    }
                                ],
                            },
                            "state": "confirmed",
                            "verification_state": "confirmed",
                            "confidence": "high",
                        },
                        {
                            "id": "contract:ui.market_tile",
                            "kind": "contract",
                            "extension_type": "ui",
                            "label": "Market Tile",
                            "columns": [],
                            "source": {},
                            "data": {},
                            "compute": {},
                            "contract": {
                                "route": "",
                                "component": "MarketTile",
                                "ui_role": "component",
                                "fields": [
                                    {
                                        "id": "field.ui_market_tile.pricing_score",
                                        "name": "pricing_score",
                                        "required": True,
                                        "primary_binding": "field.api_market_snapshot.pricing_score",
                                        "alternatives": [],
                                        "sources": [
                                            {
                                                "node_id": "contract:api.market_snapshot",
                                                "field": "pricing_score",
                                            }
                                        ],
                                    }
                                ],
                            },
                            "state": "confirmed",
                            "verification_state": "confirmed",
                            "confidence": "high",
                        },
                    ],
                    "edges": [
                        {
                            "id": "edge.binds.contract_api_market_snapshot.contract_ui_market_tile",
                            "type": "binds",
                            "source": "contract:api.market_snapshot",
                            "target": "contract:ui.market_tile",
                        }
                    ],
                },
                "updated_by": "sql-orm-coverage-test",
            },
        )
        self.assertEqual(import_response.status_code, 200)

        backend_dir = self.root / "backend"
        backend_dir.mkdir(parents=True, exist_ok=True)
        (backend_dir / "__init__.py").write_text("", encoding="utf-8")
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
        (backend_dir / "routes.py").write_text(
            """
from fastapi import APIRouter

from backend.models import MarketSignal

router = APIRouter()

@router.get("/api/markets/snapshot")
def pricing_snapshot(session):
    signal = session.query(MarketSignal).first()
    return {"pricing_score": signal.pricing_score}
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
        contradiction = next(
            item
            for item in bundle["contradictions"]
            if item["kind"] == "binding_mismatch"
            and item["field_id"] == "field.api_market_snapshot.pricing_score"
        )
        self.assertEqual(
            contradiction["new_evidence"]["primary_binding"],
            "field.analytics_market_signals.pricing_score",
        )
        binding_patch = next(
            patch
            for patch in bundle["patches"]
            if patch["type"] == "change_binding"
            and patch["field_id"] == "field.api_market_snapshot.pricing_score"
        )
        self.assertIn("code_reference", binding_patch["evidence"])
        self.assertEqual(
            binding_patch["payload"]["new_binding"],
            "field.analytics_market_signals.pricing_score",
        )
        downstream_breakage = bundle["reconciliation"]["downstream_breakage"]
        self.assertGreaterEqual(downstream_breakage["count"], 1)
        self.assertTrue(any(item["source"] == "binding_mismatch" for item in downstream_breakage["by_source"]))
        self.assertTrue(
            any(item["target_id"] == "field.api_market_snapshot.pricing_score" for item in downstream_breakage["targets"])
        )


if __name__ == "__main__":
    unittest.main()
