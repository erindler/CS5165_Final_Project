"""
churn_ml.py  -  Churn Prediction via Logistic Regression
=========================================================
Answers: "Which customers are at risk of disengaging, and how can
retention strategies address this?"

Approach
--------
1. Define churn: household made NO purchases in the final 8 weeks of
   the dataset window (recency-based label).
2. Engineer RFM + demographic features per household.
3. Train a Logistic Regression classifier (primary model).
4. Return:
     - per-household churn probabilities
     - feature correlations with churn label
     - churn rates by demographic segment (for charts)
     - model performance metrics (AUC, accuracy)
     - ROC curve coordinates

Database schema (retail.*):
  retail.transactions : basket_num, hshd_num, purchase_date, product_num,
                        spend, units, store_r, week_num, year
  retail.products     : product_num, department, commodity, brand_ty,
                        natural_organic_flag
  retail.households   : hshd_num, loyalty_flag, age_range, marital,
                        income_range, homeowner, hshd_composition,
                        hh_size, children

Connection env vars:
  PGHOST, PGDATABASE, PGUSER, PGPASSWORD, PGPORT, PGSSLMODE

CSV fallback: DATA_DIR env var pointing to folder with
  400_transactions.csv, 400_households.csv, 400_products.csv
"""

from __future__ import annotations

import glob
import os

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, roc_curve, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler


# ---------------------------------------------------------------------------
# Data loading  (same pattern as basket_ml.py)
# ---------------------------------------------------------------------------

def _pg_connect():
    import psycopg2
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        port=int(os.environ.get("PGPORT", "5432")),
        sslmode=os.environ.get("PGSSLMODE", "require"),
    )


def _load_from_db() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with _pg_connect() as conn:
        transactions = pd.read_sql(
            """
            SELECT hshd_num, basket_num, purchase_date,
                   product_num, spend, units, week_num, year
            FROM   retail.transactions
            """,
            conn,
        )
        households = pd.read_sql(
            """
            SELECT hshd_num, loyalty_flag, age_range, marital,
                   income_range, homeowner, hshd_composition,
                   hh_size, children
            FROM   retail.households
            """,
            conn,
        )
        products = pd.read_sql(
            "SELECT product_num, commodity FROM retail.products",
            conn,
        )
    return transactions, households, products


_TXN_RENAME = {"date": "purchase_date", "store_region": "store_r"}


def _load_from_csvs(csv_dir: str):
    def find(pattern: str) -> str:
        hits = glob.glob(os.path.join(csv_dir, f"*{pattern}*"))
        if not hits:
            raise FileNotFoundError(
                f"No CSV matching '*{pattern}*' in {csv_dir!r}"
            )
        return hits[0]

    def clean(df: pd.DataFrame, rename: dict = {}) -> pd.DataFrame:
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        return df.rename(columns=rename)

    transactions = clean(pd.read_csv(find("transaction"), low_memory=False), _TXN_RENAME)
    households   = clean(pd.read_csv(find("household"),  low_memory=False))
    products     = clean(pd.read_csv(find("product"),    low_memory=False))
    return transactions, households, products


def load_data(csv_dir: str = "."):
    if os.environ.get("PGHOST"):
        return _load_from_db()
    return _load_from_csvs(csv_dir)


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

# How many weeks of inactivity counts as "churned"
CHURN_WEEKS = 8

# Ordered categories for cleaner sorting in charts
_INCOME_ORDER = [
    "Under 35K", "35-49K", "50-74K", "75-99K",
    "100-150K", "150K+",
    "35K-49K", "50K-74K", "75K-99K",   # alternate spellings
]
_AGE_ORDER = [
    "19-24", "25-34", "35-44", "45-54", "55-64", "65+",
    "Under 25", "25-34", "35-44", "45-54", "55-64", "65+",
]


def build_features(
    transactions: pd.DataFrame,
    households: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """
    Returns a feature DataFrame indexed by hshd_num.
    Columns: numeric RFM features, encoded demographics, churn label.
    """
    txn = transactions.copy()
    txn.columns = txn.columns.str.lower()
    hh  = households.copy()
    hh.columns  = hh.columns.str.lower()
    prd = products.copy()
    prd.columns = prd.columns.str.lower()

    # ── Churn label  ──────────────────────────────────────────────────────────
    max_week = int(txn["week_num"].max())
    churn_cutoff = max_week - CHURN_WEEKS

    last_week = txn.groupby("hshd_num")["week_num"].max().rename("last_week")

    # ── RFM features  ─────────────────────────────────────────────────────────
    recency   = (max_week - last_week).rename("weeks_since_last")
    frequency = txn.groupby("hshd_num")["basket_num"].nunique().rename("basket_count")
    monetary  = txn.groupby("hshd_num")["spend"].sum().rename("total_spend")
    avg_basket = (monetary / frequency).rename("avg_basket_spend")

    # ── Spend trend: first half vs second half ─────────────────────────────────
    mid_week  = (txn["week_num"].min() + max_week) // 2
    spend_h1  = txn[txn["week_num"] <= mid_week].groupby("hshd_num")["spend"].sum()
    spend_h2  = txn[txn["week_num"] >  mid_week].groupby("hshd_num")["spend"].sum()
    # positive = spending more recently (good sign), negative = declining
    spend_trend = (spend_h2.subtract(spend_h1, fill_value=0)).rename("spend_trend")

    # ── Category diversity ────────────────────────────────────────────────────
    if "commodity" not in txn.columns:
        txn = txn.merge(prd[["product_num", "commodity"]], on="product_num", how="left")
    diversity = txn.groupby("hshd_num")["commodity"].nunique().rename("commodity_diversity")

    # ── Assemble numeric frame ─────────────────────────────────────────────────
    feat = pd.concat(
        [recency, frequency, monetary, avg_basket, spend_trend, diversity],
        axis=1,
    )

    # ── Demographics ──────────────────────────────────────────────────────────
    hh = hh.set_index("hshd_num")

    # Loyalty flag: Y -> 1, else 0
    feat["loyalty"] = hh["loyalty_flag"].map(
        lambda x: 1 if str(x).strip().upper() == "Y" else 0
    )

    # Ordinal encode age, income  (unknown -> -1)
    def safe_label_encode(series: pd.Series) -> pd.Series:
        le = LabelEncoder()
        filled = series.fillna("Unknown").astype(str).str.strip()
        le.fit(filled)
        return pd.Series(le.transform(filled), index=series.index, dtype=float)

    for col in ["age_range", "income_range", "hh_size", "children", "marital"]:
        if col in hh.columns:
            feat[col] = safe_label_encode(hh[col])

    # ── Churn label ───────────────────────────────────────────────────────────
    feat["churned"] = (last_week <= churn_cutoff).astype(int)

    return feat.dropna(subset=["basket_count", "total_spend"])


# ---------------------------------------------------------------------------
# Correlation analysis
# ---------------------------------------------------------------------------

def compute_correlations(feat: pd.DataFrame) -> list[dict]:
    """
    Pearson correlation of every feature column with the churn label.
    Returns list sorted by absolute correlation descending.
    """
    feature_cols = [c for c in feat.columns if c != "churned"]
    corrs = feat[feature_cols].corrwith(feat["churned"])
    result = [
        {"feature": col, "correlation": round(float(corrs[col]), 4)}
        for col in feature_cols
        if not np.isnan(corrs[col])
    ]
    result.sort(key=lambda x: abs(x["correlation"]), reverse=True)
    return result


# ---------------------------------------------------------------------------
# Segment analysis  (for charts)
# ---------------------------------------------------------------------------

def compute_segment_rates(
    feat: pd.DataFrame,
    households: pd.DataFrame,
) -> dict[str, list[dict]]:
    """
    Churn rate by key demographic segments.
    Returns a dict of segment_name -> list of {label, churn_rate, count}.
    """
    hh = households.copy()
    hh.columns = hh.columns.str.lower()
    hh = hh.set_index("hshd_num")

    segments: dict[str, list[dict]] = {}

    def rate_for(col: str) -> list[dict]:
        if col not in hh.columns:
            return []
        merged = feat[["churned"]].join(hh[[col]])
        merged[col] = merged[col].fillna("Unknown").astype(str).str.strip()
        grp = merged.groupby(col)["churned"].agg(["mean", "count"]).reset_index()
        grp.columns = [col, "churn_rate", "count"]
        grp = grp[grp["count"] >= 5]   # skip tiny groups
        rows = [
            {
                "label":      str(row[col]),
                "churn_rate": round(float(row["churn_rate"]) * 100, 1),
                "count":      int(row["count"]),
            }
            for _, row in grp.iterrows()
        ]
        rows.sort(key=lambda x: x["label"])
        return rows

    for col in ["age_range", "income_range", "loyalty_flag",
                "hshd_composition", "children"]:
        rows = rate_for(col)
        if rows:
            segments[col] = rows

    return segments


# ---------------------------------------------------------------------------
# Logistic Regression model
# ---------------------------------------------------------------------------

def train_churn_model(feat: pd.DataFrame) -> dict:
    """
    Trains Logistic Regression on RFM + demographic features.
    Returns model metrics, coefficients, ROC curve data, and
    per-household churn probabilities for the highest-risk customers.
    """
    feature_cols = [c for c in feat.columns if c != "churned"]
    X = feat[feature_cols].fillna(0).to_numpy()
    y = y = feat["churned"].to_numpy()

    if y.sum() < 5 or (len(y) - y.sum()) < 5:
        return {"error": "Not enough churned / non-churned households to train."}

    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_tr_s = scaler.fit_transform(X_tr)
    X_te_s = scaler.transform(X_te)

    clf = LogisticRegression(
        class_weight="balanced",
        max_iter=1000,
        random_state=42,
    )
    clf.fit(X_tr_s, y_tr)

    y_prob = clf.predict_proba(X_te_s)[:, 1]
    y_pred = clf.predict(X_te_s)

    auc      = round(float(roc_auc_score(y_te, y_prob)), 3)
    accuracy = round(float(accuracy_score(y_te, y_pred)), 3)

    # ROC curve (downsample to ~40 points for JSON compactness)
    fpr, tpr, _ = roc_curve(y_te, y_prob)
    step = max(1, len(fpr) // 40)
    roc_data = [
        {"fpr": round(float(fpr[i]), 3), "tpr": round(float(tpr[i]), 3)}
        for i in range(0, len(fpr), step)
    ]

    # Feature coefficients (scaled, so they're comparable)
    coefs = [
        {
            "feature": feature_cols[i],
            "coefficient": round(float(clf.coef_[0][i]), 4),
        }
        for i in range(len(feature_cols))
    ]
    coefs.sort(key=lambda x: abs(x["coefficient"]), reverse=True)

    # Top at-risk households (full dataset, not just test split)
    X_all_s = scaler.transform(feat[feature_cols].fillna(0).values)
    probs    = clf.predict_proba(X_all_s)[:, 1]
    risk_df  = feat[["churned"]].copy()
    risk_df["churn_prob"] = probs
    at_risk  = (
        risk_df[risk_df["churned"] == 0]   # not yet churned
        .sort_values("churn_prob", ascending=False)
        .head(20)
    )
    at_risk_list = [
        {
            "hshd_num": idx,
            "churn_prob": round(float(row["churn_prob"]) * 100, 1),
        }
        for idx, row in at_risk.iterrows()
    ]

    return {
        "auc":          auc,
        "accuracy":     accuracy,
        "churn_rate":   round(float(y.mean()) * 100, 1),
        "n_churned":    int(y.sum()),
        "n_total":      int(len(y)),
        "roc_data":     roc_data,
        "coefficients": coefs,
        "at_risk":      at_risk_list,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_churn_analysis(csv_dir: str = ".") -> dict:
    """
    Full pipeline: load -> engineer -> correlate -> segment -> model.
    Returns a JSON-serialisable dict for the Flask route.
    """
    transactions, households, products = load_data(csv_dir)
    feat     = build_features(transactions, households, products)
    corrs    = compute_correlations(feat)
    segments = compute_segment_rates(feat, households)
    model    = train_churn_model(feat)

    return {
        "n_households": int(len(feat)),
        "churn_weeks":  CHURN_WEEKS,
        "correlations": corrs,
        "segments":     segments,
        "model":        model,
    }