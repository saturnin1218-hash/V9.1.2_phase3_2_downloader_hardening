from __future__ import annotations

import json
import logging
from pathlib import Path

from binance_quant_builder.observability import setup_run_logger
from binance_quant_builder.runtime import PipelineRuntime, RetryPolicy


def test_runtime_retries_timeout_once(tmp_path: Path):
    logger = setup_run_logger('run-test-retry', tmp_path, level=logging.INFO)
    runtime = PipelineRuntime(run_id='run-test-retry', logger=logger)
    state = {'n': 0}

    def flaky():
        state['n'] += 1
        if state['n'] == 1:
            raise TimeoutError('temporary timeout')
        output = tmp_path / 'ok.json'
        output.write_text('{}', encoding='utf-8')
        return {'output_path': str(output)}

    result = runtime.run_stage('download-only', flaky, retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0.0, retry_on=(TimeoutError,)))
    assert Path(result['output_path']).exists()
    metrics = runtime.execution_metrics()
    assert metrics['retry_count'] == 1
    assert metrics['stages'][0]['attempt_count'] == 2


def test_retry_event_is_logged(tmp_path: Path):
    logger = setup_run_logger('run-test-log', tmp_path, level=logging.INFO)
    runtime = PipelineRuntime(run_id='run-test-log', logger=logger)
    state = {'n': 0}

    def flaky():
        state['n'] += 1
        if state['n'] == 1:
            raise TimeoutError('boom')
        return {}

    runtime.run_stage('feature-only', flaky, retry_policy=RetryPolicy(max_attempts=2, backoff_seconds=0.0, retry_on=(TimeoutError,)))
    lines = (tmp_path / 'run.jsonl').read_text(encoding='utf-8').strip().splitlines()
    events = [json.loads(line)['event'] for line in lines]
    assert 'stage_retry_scheduled' in events
