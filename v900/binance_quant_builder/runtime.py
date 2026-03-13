from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .errors import RetryExhaustedError
from .ids import make_stage_id
from .manifests import file_metadata
from .observability import log_event


@dataclass(slots=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff_seconds: float = 0.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 5.0
    retry_on: tuple[type[BaseException], ...] = tuple()


@dataclass(slots=True)
class StageAttempt:
    attempt: int
    status: str
    duration_ms: int
    error: str | None = None


@dataclass(slots=True)
class StageRecord:
    stage: str
    stage_id: str
    status: str
    duration_ms: int
    result: dict[str, Any] | None = None
    error: str | None = None
    attempts: list[StageAttempt] = field(default_factory=list)


def _extract_artifact_paths(obj: Any) -> list[str]:
    paths: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key.endswith('path') and isinstance(item, str) and Path(item).exists():
                    paths.append(item)
                else:
                    walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(obj)
    return sorted(set(paths))


@dataclass(slots=True)
class PipelineRuntime:
    run_id: str
    logger: logging.Logger
    stages: list[StageRecord] = field(default_factory=list)

    def run_stage(
        self,
        stage_name: str,
        fn: Callable[[], dict[str, Any]],
        *,
        retry_policy: RetryPolicy | None = None,
        input_paths: list[str] | None = None,
    ) -> dict[str, Any]:
        stage_id = make_stage_id(self.run_id, stage_name)
        policy = retry_policy or RetryPolicy()
        log_event(self.logger, logging.INFO, 'stage_started', run_id=self.run_id, stage=stage_name, stage_id=stage_id, input_paths=input_paths or [])

        attempts: list[StageAttempt] = []
        delay = float(policy.backoff_seconds)
        total_start = time.perf_counter()
        last_exc: BaseException | None = None

        for attempt in range(1, int(policy.max_attempts) + 1):
            t0 = time.perf_counter()
            try:
                result = fn() or {}
                dt = int((time.perf_counter() - t0) * 1000)
                attempts.append(StageAttempt(attempt=attempt, status='ok', duration_ms=dt))
                total_dt = int((time.perf_counter() - total_start) * 1000)
                record = StageRecord(stage=stage_name, stage_id=stage_id, status='ok', duration_ms=total_dt, result=result, attempts=attempts)
                self.stages.append(record)
                log_event(
                    self.logger, logging.INFO, 'stage_completed', run_id=self.run_id, stage=stage_name, stage_id=stage_id,
                    duration_ms=total_dt, attempt=attempt, output_paths=_extract_artifact_paths(result)
                )
                return result
            except Exception as exc:  # noqa: BLE001
                dt = int((time.perf_counter() - t0) * 1000)
                attempts.append(StageAttempt(attempt=attempt, status='error', duration_ms=dt, error=str(exc)))
                last_exc = exc
                retryable = isinstance(exc, policy.retry_on)
                can_retry = retryable and attempt < int(policy.max_attempts)
                log_event(
                    self.logger, logging.WARNING if can_retry else logging.ERROR,
                    'stage_retry_scheduled' if can_retry else 'stage_failed',
                    run_id=self.run_id, stage=stage_name, stage_id=stage_id, duration_ms=dt, attempt=attempt,
                    exception_type=type(exc).__name__, error=str(exc), retryable=retryable, next_delay_s=delay if can_retry else None,
                )
                if can_retry:
                    time.sleep(delay)
                    delay = min(max(delay * float(policy.backoff_multiplier), 0.0), float(policy.max_backoff_seconds))
                    continue
                total_dt = int((time.perf_counter() - total_start) * 1000)
                self.stages.append(StageRecord(stage=stage_name, stage_id=stage_id, status='error', duration_ms=total_dt, error=str(exc), attempts=attempts))
                if retryable and attempt >= int(policy.max_attempts):
                    raise RetryExhaustedError(f"Stage {stage_name} failed after {attempt} attempts: {exc}") from exc
                raise

        raise RetryExhaustedError(f"Stage {stage_name} failed after {policy.max_attempts} attempts: {last_exc}")

    def execution_metrics(self) -> dict[str, Any]:
        total_stages = len(self.stages)
        failed_stages = sum(1 for s in self.stages if s.status != 'ok')
        retry_count = sum(max(len(s.attempts) - 1, 0) for s in self.stages)
        total_duration_ms = sum(s.duration_ms for s in self.stages)
        return {
            'total_stages': total_stages,
            'failed_stages': failed_stages,
            'retry_count': retry_count,
            'total_duration_ms': total_duration_ms,
            'stages': [
                {
                    'stage': s.stage,
                    'stage_id': s.stage_id,
                    'status': s.status,
                    'duration_ms': s.duration_ms,
                    'attempt_count': len(s.attempts),
                    'output_paths': _extract_artifact_paths(s.result or {}),
                }
                for s in self.stages
            ],
        }

    def stage_lineage(self) -> list[dict[str, Any]]:
        lineage: list[dict[str, Any]] = []
        for s in self.stages:
            outputs = []
            for path in _extract_artifact_paths(s.result or {}):
                try:
                    outputs.append(file_metadata(path))
                except FileNotFoundError:
                    continue
            lineage.append({
                'stage': s.stage,
                'stage_id': s.stage_id,
                'status': s.status,
                'attempt_count': len(s.attempts),
                'outputs': outputs,
            })
        return lineage
