from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataguard_core.store import postgres, redis
from dataguard_core.store.postgres import RemediationAuditRow, RemediationPlanRow
from dataguard_core.store.redis import LockAcquisitionError
from dataguard_core.store.repositories import AuditRepository, IncidentRepository, RemediationRepository


class TestRedisStore:
    @pytest.mark.asyncio
    async def test_redis_init_and_get(self) -> None:
        with patch("dataguard_core.store.redis.aioredis.from_url") as mock_from_url:
            mock_client = MagicMock()
            mock_from_url.return_value = mock_client
            redis.init_redis("redis://localhost:6379/1")
            assert redis.get_redis() == mock_client

    @pytest.mark.asyncio
    async def test_cache_get_set_delete(self) -> None:
        mock_client = AsyncMock()
        mock_client.get.return_value = '{"foo": "bar"}'
        with patch("dataguard_core.store.redis.get_redis", return_value=mock_client):
            val = await redis.cache_get("key1")
            assert val == {"foo": "bar"}

            await redis.cache_set("key2", {"a": 1}, ttl=10)
            mock_client.setex.assert_called_once()

            await redis.cache_delete("key3")
            mock_client.delete.assert_called_once_with("key3")

    @pytest.mark.asyncio
    async def test_acquire_and_release_lock(self) -> None:
        mock_client = AsyncMock()
        mock_client.set.return_value = True
        mock_client.eval.return_value = 1

        with patch("dataguard_core.store.redis.get_redis", return_value=mock_client):
            token = await redis.acquire_lock("resource_1")
            assert token is not None

            released = await redis.release_lock("resource_1", token)
            assert released is True

    @pytest.mark.asyncio
    async def test_distributed_lock_context_manager(self) -> None:
        mock_client = AsyncMock()
        mock_client.set.return_value = True
        mock_client.eval.return_value = 1

        with patch("dataguard_core.store.redis.get_redis", return_value=mock_client):
            async with redis.distributed_lock("resource_cm") as token:
                assert token is not None

    @pytest.mark.asyncio
    async def test_distributed_lock_failure_raises(self) -> None:
        mock_client = AsyncMock()
        mock_client.set.return_value = False

        with (
            patch("dataguard_core.store.redis.get_redis", return_value=mock_client),
            pytest.raises(LockAcquisitionError),
        ):
            async with redis.distributed_lock("busy_resource"):
                pass


class TestPostgresStore:
    def test_get_engine_uninitialized_raises(self) -> None:
        postgres._engine = None
        with pytest.raises(RuntimeError):
            postgres.get_engine()

    def test_init_engine(self) -> None:
        with patch("dataguard_core.store.postgres.create_async_engine") as mock_create:
            mock_engine = MagicMock()
            mock_create.return_value = mock_engine
            postgres.init_engine("postgresql+asyncpg://user:pass@localhost/db")
            assert postgres.get_engine() == mock_engine

    @pytest.mark.asyncio
    async def test_dispose_engine(self) -> None:
        mock_engine = AsyncMock()
        postgres._engine = mock_engine
        await postgres.dispose_engine()
        assert postgres._engine is None


class TestRepositories:
    @pytest.mark.asyncio
    async def test_incident_repository_file_and_get(self) -> None:
        repo = IncidentRepository()
        mock_session = AsyncMock()
        mock_session.add = MagicMock()

        with patch("dataguard_core.store.repositories.get_session") as mock_get_sess:
            mock_get_sess.return_value.__aenter__.return_value = mock_session

            inc = await repo.file_incident(
                title="Test failure",
                pipeline_id="pipe-1",
                severity="high",
                description="OOM error",
                diagnosis_id="diag-1",
                root_cause_category="oom",
            )
            assert inc.title == "Test failure"
            mock_session.add.assert_called_once()

    @pytest.mark.asyncio
    async def test_remediation_repository_save_and_get(self) -> None:
        repo = RemediationRepository()
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_plan = RemediationPlanRow(
            id="REM-1",
            diagnosis_id="D-1",
            pipeline_id="P-1",
            steps_json="[]",
            risk_level="low",
            rollback_plan="rollback",
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_plan
        mock_session.execute.return_value = mock_result

        with patch("dataguard_core.store.repositories.get_session") as mock_get_sess:
            mock_get_sess.return_value.__aenter__.return_value = mock_session
            await repo.save_plan(mock_plan)
            mock_session.add.assert_called_once_with(mock_plan)

            plan = await repo.get_plan("REM-1")
            assert plan == mock_plan

    @pytest.mark.asyncio
    async def test_audit_repository_record(self) -> None:
        repo = AuditRepository()
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        audit = RemediationAuditRow(
            id="AUD-1",
            remediation_id="REM-1",
            pipeline_id="P-1",
            approver_id="alice",
            risk_level="low",
            actions_taken_json="[]",
            outcome="success",
        )

        with patch("dataguard_core.store.repositories.get_session") as mock_get_sess:
            mock_get_sess.return_value.__aenter__.return_value = mock_session
            await repo.record_audit(audit)
            mock_session.add.assert_called_once_with(audit)
