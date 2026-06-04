"""Tabular data providers: sample and synthetic.

Requires the 'data' extra (pandas, numpy). Imported lazily by providers/__init__.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import DataProvider


def _read(path: str, jdbc: str = None, table: str = None, limit: int = None) -> pd.DataFrame:
    if jdbc:
        import sqlalchemy
        engine = sqlalchemy.create_engine(jdbc)
        q = f"SELECT * FROM {table}"
        if limit:
            q += f" LIMIT {limit}"
        return pd.read_sql(q, engine)
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    if path.endswith(".json"):
        return pd.read_json(path, lines=True)
    return pd.read_csv(path)


def _write(df: pd.DataFrame, dest: str) -> str:
    if dest.endswith(".parquet"):
        df.to_parquet(dest, index=False)
    elif dest.endswith(".json"):
        df.to_json(dest, orient="records", lines=True)
    else:
        df.to_csv(dest, index=False)
    return dest


class SampleProvider(DataProvider):
    """Copy a subset of real data. Exact values preserved."""

    name = "sample"

    def materialize(self, source: str, dest: str, **opts) -> str:
        pct = float(opts.get("pct", 1.0))
        seed = int(opts.get("seed", 42))
        jdbc = opts.get("jdbc")
        table = opts.get("table")

        if jdbc:
            # DB-side sampling where the dialect supports random()
            import sqlalchemy
            engine = sqlalchemy.create_engine(jdbc)
            frac = max(min(pct / 100.0, 1.0), 0.0)
            try:
                df = pd.read_sql(
                    f"SELECT * FROM {table} WHERE random() < {frac}", engine
                )
            except Exception:
                df = pd.read_sql(f"SELECT * FROM {table}", engine).sample(
                    frac=frac, random_state=seed
                )
        else:
            df = _read(source)
            frac = max(min(pct / 100.0, 1.0), 0.0)
            df = df.sample(frac=frac, random_state=seed)
        return _write(df, dest)


class SyntheticProvider(DataProvider):
    """Learn column shapes from real data, generate new rows that match."""

    name = "synthetic"

    @staticmethod
    def _profile(series: pd.Series) -> dict:
        p = {"name": series.name, "null_rate": float(series.isna().mean())}
        s = series.dropna()
        if pd.api.types.is_numeric_dtype(s):
            p.update(
                kind="numeric",
                is_int=bool(pd.api.types.is_integer_dtype(s)),
                min=float(s.min()) if len(s) else 0.0,
                max=float(s.max()) if len(s) else 0.0,
                mean=float(s.mean()) if len(s) else 0.0,
                std=float(s.std()) if len(s) > 1 else 0.0,
            )
        elif pd.api.types.is_datetime64_any_dtype(s):
            p.update(kind="datetime", min=s.min(), max=s.max())
        else:
            vc = s.astype(str).value_counts(normalize=True).head(50)
            p.update(kind="categorical", values=list(vc.index), probs=list(vc.values))
        return p

    @staticmethod
    def _gen(p: dict, n: int, rng) -> pd.Series:
        if p["kind"] == "numeric":
            if p["std"] > 0:
                vals = rng.normal(p["mean"], p["std"], n)
            else:
                vals = np.full(n, p["mean"])
            vals = np.clip(vals, p["min"], p["max"])
            if p.get("is_int"):
                vals = np.round(vals).astype("int64")
            out = pd.Series(vals)
        elif p["kind"] == "datetime":
            lo, hi = pd.Timestamp(p["min"]).value, pd.Timestamp(p["max"]).value
            hi = hi if hi > lo else lo + 1
            out = pd.Series(pd.to_datetime(rng.integers(lo, hi, n)))
        else:
            vals = p["values"] or ["NA"]
            probs = np.array(p["probs"]) if p["probs"] else None
            if probs is not None:
                probs = probs / probs.sum()
            out = pd.Series(rng.choice(vals, size=n, p=probs))
        if p["null_rate"] > 0:
            out[rng.random(n) < p["null_rate"]] = np.nan
        out.name = p["name"]
        return out

    def materialize(self, source: str, dest: str, **opts) -> str:
        rows = int(opts.get("rows", 100000))
        seed = int(opts.get("seed", 42))
        df = _read(source, opts.get("jdbc"), opts.get("table"), opts.get("limit"))
        rng = np.random.default_rng(seed)
        profiles = [self._profile(df[c]) for c in df.columns]
        synth = pd.DataFrame({p["name"]: self._gen(p, rows, rng) for p in profiles})
        return _write(synth, dest)
