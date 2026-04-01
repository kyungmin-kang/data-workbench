from __future__ import annotations

import time
import unittest
from typing import Any

from fastapi.testclient import TestClient

from workbench.app import app
from tests.workbench_test_support import WorkbenchTempRootMixin, make_valid_plan_state


class ApiTestCase(WorkbenchTempRootMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.setUpWorkbenchRoot()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.tearDownWorkbenchRoot()

    def wait_for_project_profile_job(self, job_id: str) -> dict[str, Any]:
        for _ in range(200):
            response = self.client.get(f"/api/project/profile/jobs/{job_id}")
            self.assertEqual(response.status_code, 200)
            job = response.json()["job"]
            if job["status"] in {"completed", "failed"}:
                return job
            time.sleep(0.01)
        self.fail(f"Timed out waiting for project profile job {job_id}")

    def wait_for_project_asset_profile_job(self, job_id: str) -> dict[str, Any]:
        for _ in range(200):
            response = self.client.get(f"/api/project/profile/assets/jobs/{job_id}")
            self.assertEqual(response.status_code, 200)
            job = response.json()["job"]
            if job["status"] in {"completed", "failed"}:
                return job
            time.sleep(0.01)
        self.fail(f"Timed out waiting for project asset profile job {job_id}")

    def make_valid_plan_state(
        self,
        *,
        decision_status: str = "accepted",
        task_status: str = "todo",
    ) -> dict[str, Any]:
        return make_valid_plan_state(decision_status=decision_status, task_status=task_status)
