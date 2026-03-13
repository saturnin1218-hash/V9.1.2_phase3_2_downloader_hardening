from __future__ import annotations


class PipelineError(Exception):
    """Base class for pipeline errors."""


class ConfigValidationError(PipelineError):
    pass


class ContractValidationError(PipelineError):
    pass


class QualityGateError(PipelineError):
    pass


class RetryableAPIError(PipelineError):
    pass


class FatalDataError(PipelineError):
    pass


class ArtifactWriteError(PipelineError):
    pass


class RetryExhaustedError(PipelineError):
    pass
