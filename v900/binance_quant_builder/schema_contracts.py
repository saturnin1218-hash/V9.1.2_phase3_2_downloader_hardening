from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from .errors import ContractValidationError
from .utils.time import ensure_utc_timestamp


@dataclass(slots=True)
class ContractResult:
    name: str
    passed: bool
    errors: list[str]
    rows: int
    columns: list[str]


def _finalize(name: str, df: pd.DataFrame, errors: list[str]) -> ContractResult:
    return ContractResult(name=name, passed=not errors, errors=errors, rows=int(len(df)), columns=list(df.columns))


def _check_required(df: pd.DataFrame, required: list[str], errors: list[str]) -> None:
    for col in required:
        if col not in df.columns:
            errors.append(f"colonne obligatoire absente: {col}")


def _check_timestamp(df: pd.DataFrame, errors: list[str], key: str = "timestamp") -> None:
    if key not in df.columns:
        return
    ts = ensure_utc_timestamp(df[key])
    if ts.isna().any():
        errors.append("timestamp contient des valeurs invalides")
        return
    if not ts.is_monotonic_increasing:
        errors.append("timestamp n'est pas monotone croissant")
    if ts.duplicated().any():
        errors.append("timestamp contient des doublons")


def validate_trades_contract(df: pd.DataFrame) -> ContractResult:
    errors: list[str] = []
    _check_required(df, ["timestamp", "price"], errors)
    if "quantity" not in df.columns and "qty" not in df.columns:
        errors.append("quantity ou qty obligatoire")
    if not errors:
        _check_timestamp(df, errors)
        price = pd.to_numeric(df["price"], errors="coerce")
        if price.isna().any() or (price <= 0).any():
            errors.append("price doit être strictement positif")
        q = pd.to_numeric(df["quantity"] if "quantity" in df.columns else df["qty"], errors="coerce")
        if q.isna().any() or (q < 0).any():
            errors.append("quantity/qty doit être >= 0")
    return _finalize("trades", df, errors)


def validate_feature_dataset_contract(df: pd.DataFrame) -> ContractResult:
    errors: list[str] = []
    _check_required(df, ["timestamp", "open", "high", "low", "close", "label_tb"], errors)
    if not errors:
        _check_timestamp(df, errors)
        for col in ["open", "high", "low", "close"]:
            s = pd.to_numeric(df[col], errors="coerce")
            if s.isna().any() or (s <= 0).any():
                errors.append(f"{col} doit être strictement positif")
        if {"high", "low"}.issubset(df.columns):
            hi = pd.to_numeric(df["high"], errors="coerce")
            lo = pd.to_numeric(df["low"], errors="coerce")
            if (hi < lo).any():
                errors.append("high doit être >= low")
    return _finalize("feature_dataset", df, errors)


def validate_merged_dataset_contract(df: pd.DataFrame) -> ContractResult:
    result = validate_feature_dataset_contract(df)
    errors = list(result.errors)
    if "sum_open_interest" in df.columns:
        oi = pd.to_numeric(df["sum_open_interest"], errors="coerce")
        if (oi.dropna() < 0).any():
            errors.append("sum_open_interest doit être >= 0")
    if "liquidation_qty_sum" in df.columns:
        liq = pd.to_numeric(df["liquidation_qty_sum"], errors="coerce")
        if (liq.dropna() < 0).any():
            errors.append("liquidation_qty_sum doit être >= 0")
    return _finalize("merged_dataset", df, errors)


def raise_on_contract_failure(result: ContractResult) -> None:
    if not result.passed:
        raise ContractValidationError(f"Contrat {result.name} invalide: " + " | ".join(result.errors))
