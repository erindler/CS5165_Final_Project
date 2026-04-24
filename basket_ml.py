"""
basket_ml.py  -  Basket Analysis via Random Forest
====================================================
Answers: "What are commonly purchased product combinations,
and how can they drive cross-selling opportunities?"

Database schema (retail.*):
  retail.transactions  : basket_num, hshd_num, purchase_date, product_num,
                         spend, units, store_r, week_num, year
  retail.products      : product_num, department, commodity,
                         brand_ty, natural_organic_flag
  retail.households    : hshd_num, loyalty_flag, age_range, marital,
                         income_range, homeowner, hshd_composition,
                         hh_size, children

Connection env vars (Azure App Service -> Configuration):
  PGHOST      e.g. myserver.postgres.database.azure.com
  PGDATABASE
  PGUSER
  PGPASSWORD
  PGPORT      (optional, default 5432)
  PGSSLMODE   (optional, default 'require')

CSV fallback (local dev):
  Set DATA_DIR to a folder containing 400_transactions.csv and
  400_products.csv.  Column names are mapped automatically.
"""

from __future__ import annotations

import glob
import os

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _pg_connect():
    """Return a raw psycopg2 connection — no SQLAlchemy needed."""
    import psycopg2
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        port=int(os.environ.get("PGPORT", "5432")),
        sslmode=os.environ.get("PGSSLMODE", "require"),
    )


def _load_from_db() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load from Azure PostgreSQL using the retail.* schema.
    commodity lives in retail.products so we JOIN it onto transactions.
    Uses psycopg2 directly — no SQLAlchemy required.
    """
    with _pg_connect() as conn:
        transactions = pd.read_sql(
            """
            SELECT t.hshd_num,
                   t.basket_num,
                   t.product_num,
                   t.spend,
                   t.units,
                   t.store_r,
                   t.week_num,
                   t.year,
                   p.department,
                   p.commodity
            FROM   retail.transactions t
            JOIN   retail.products p USING (product_num)
            """,
            conn,
        )
        products = pd.read_sql(
            """
            SELECT product_num, department, commodity,
                   brand_ty, natural_organic_flag
            FROM   retail.products
            """,
            conn,
        )
    return transactions, products


# Maps raw CSV headers -> DB column names
_TXN_RENAME = {
    "date":         "purchase_date",
    "store_region": "store_r",
}
_PRD_RENAME: dict = {}  # product CSV headers already match DB names


def _load_from_csvs(csv_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    CSV fallback for local development.
    Reads 400_transactions.csv and 400_products.csv, normalises headers,
    and joins commodity onto the transactions frame so both load paths
    return the same shape.
    """
    def find(pattern: str) -> str:
        hits = glob.glob(os.path.join(csv_dir, f"*{pattern}*"))
        if not hits:
            raise FileNotFoundError(
                f"No CSV matching '*{pattern}*' found in {csv_dir!r}"
            )
        return hits[0]

    def clean_cols(df: pd.DataFrame, rename: dict) -> pd.DataFrame:
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        return df.rename(columns=rename)

    transactions = clean_cols(
        pd.read_csv(find("transaction"), low_memory=False), _TXN_RENAME
    )
    products = clean_cols(
        pd.read_csv(find("product"), low_memory=False), _PRD_RENAME
    )

    # Join commodity onto transactions (mirrors the SQL JOIN in _load_from_db)
    if "commodity" not in transactions.columns:
        transactions = transactions.merge(
            products[["product_num", "department", "commodity"]],
            on="product_num",
            how="left",
        )

    return transactions, products


def load_data(csv_dir: str = ".") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use PostgreSQL when PGHOST is set, otherwise fall back to CSVs."""
    if os.environ.get("PGHOST"):
        return _load_from_db()
    return _load_from_csvs(csv_dir)


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def build_household_commodity_matrix(
    transactions: pd.DataFrame,
    min_households: int = 10,
) -> pd.DataFrame:
    """
    Returns a binary DataFrame  rows=hshd_num, cols=commodity.
    Value 1 = household ever purchased that commodity.
    Commodities bought by fewer than min_households households are dropped.
    """
    merged = transactions[["hshd_num", "commodity"]].copy()
    merged["commodity"] = merged["commodity"].astype(str).str.strip()

    pivot = (
        merged.groupby(["hshd_num", "commodity"])
        .size()
        .unstack(fill_value=0)
        .clip(upper=1)
        .astype(np.int8)
    )

    keep = pivot.columns[pivot.sum() >= min_households]
    return pivot[keep]


# ---------------------------------------------------------------------------
# Co-occurrence baseline
# ---------------------------------------------------------------------------

def compute_cooccurrence(matrix: pd.DataFrame, top_n: int = 20) -> list[dict]:
    """
    Counts households that purchased BOTH commodity A and B.
    Returns the top_n pairs by joint count.
    """
    arr  = matrix.values
    cols = matrix.columns.tolist()
    pairs: list[tuple[int, str, str]] = []

    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            joint = int((arr[:, i] & arr[:, j]).sum())
            if joint > 0:
                pairs.append((joint, cols[i], cols[j]))

    pairs.sort(reverse=True)
    return [
        {"commodity_a": a, "commodity_b": b, "joint_households": cnt}
        for cnt, a, b in pairs[:top_n]
    ]


# ---------------------------------------------------------------------------
# Random Forest cross-sell models
# ---------------------------------------------------------------------------

def train_crosssell_models(
    matrix: pd.DataFrame,
    top_commodities: int = 15,
    n_estimators: int = 100,
    random_state: int = 42,
) -> list[dict]:
    """
    For each of the top_commodities most-purchased commodities, trains a
    Random Forest Classifier:
        target   = "did this household buy commodity X?"
        features = purchase flags for all other commodities

    Feature importances identify the strongest co-purchase predictors.
    Returns results sorted by AUC descending (best cross-sell signal first).
    """
    targets = (
        matrix.sum()
        .sort_values(ascending=False)
        .head(top_commodities)
        .index.tolist()
    )

    results: list[dict] = []
    for target in targets:
        y             = matrix[target].values
        X             = matrix.drop(columns=[target]).values
        feature_names = matrix.drop(columns=[target]).columns.tolist()

        # Need at least 5 positives and 5 negatives
        if y.sum() < 5 or (len(y) - y.sum()) < 5:
            continue

        X_tr, X_te, y_tr, y_te = train_test_split(
            X, y, test_size=0.25, random_state=random_state, stratify=y
        )

        clf = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=6,
            class_weight="balanced",
            random_state=random_state,
            n_jobs=-1,
        )
        clf.fit(X_tr, y_tr)

        try:
            auc = round(float(roc_auc_score(y_te, clf.predict_proba(X_te)[:, 1])), 3)
        except Exception:
            auc = 0.0

        importances   = clf.feature_importances_
        top_idx       = np.argsort(importances)[::-1][:5]
        top_predictors = [
            {"commodity": feature_names[i], "importance": round(float(importances[i]), 4)}
            for i in top_idx
            if importances[i] > 0
        ]

        results.append({"target": target, "auc": auc, "top_predictors": top_predictors})

    results.sort(key=lambda x: x["auc"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_basket_analysis(csv_dir: str = ".") -> dict:
    """
    Full pipeline: load -> feature engineer -> model.
    Returns a JSON-serialisable dict for the Flask route.
    """
    transactions, products = load_data(csv_dir)
    matrix       = build_household_commodity_matrix(transactions)
    cooccurrence = compute_cooccurrence(matrix, top_n=20)
    crosssell    = train_crosssell_models(matrix, top_commodities=15)

    return {
        "n_households":  int(matrix.shape[0]),
        "n_commodities": int(matrix.shape[1]),
        "cooccurrence":  cooccurrence,
        "crosssell":     crosssell,
    }