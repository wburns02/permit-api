#!/usr/bin/env python3
"""
Predictive Permit Analytics — XGBoost Training Pipeline

Runs on R730 (88 cores, 755GB RAM) against T430 PostgreSQL (744M permits).
Trains a binary classifier to predict which ZIP codes will see 5+ new permits
in the next 90 days, then batch-scores all active ZIPs.

Requirements:
    pip install xgboost scikit-learn pandas psycopg2-binary joblib

Usage:
    python train_predictive_model.py
    python train_predictive_model.py --db-host 100.122.216.15
    python train_predictive_model.py --db-host localhost --db-port 5432
"""

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    precision_recall_fscore_support,
    roc_auc_score,
)
from xgboost import XGBClassifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL_VERSION = f"v1_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
MODEL_OUTPUT_PATH = "/home/will/permit_predict_model.joblib"

# Feature names for the model (order matters for consistency)
FEATURE_NAMES = [
    "permit_count_30d",
    "permit_count_90d",
    "permit_count_365d",
    "yoy_acceleration",
    "avg_monthly_permits",
    "seasonality_factor",
    "pct_residential",
    "median_sale_price",
    "price_yoy_change",
    "median_dom",
    "inventory",
    "median_income",
    "homeownership_rate",
    "median_year_built",
]


def get_connection(args):
    """Create a psycopg2 connection to the T430 PostgreSQL database."""
    logger.info("Connecting to PostgreSQL at %s:%s/%s", args.db_host, args.db_port, args.db_name)
    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        connect_timeout=30,
    )
    conn.autocommit = False
    return conn


def get_active_zips(conn):
    """Get ZIP codes with 10+ total permits (active ZIPs worth scoring)."""
    logger.info("Finding active ZIPs with 10+ permits...")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT zip_code AS zip, COUNT(*) as cnt
            FROM permits
            WHERE zip_code IS NOT NULL AND zip_code != ''
            GROUP BY zip_code
            HAVING COUNT(*) >= 10
        """)
        rows = cur.fetchall()
    elapsed = time.time() - t0
    logger.info("Found %d active ZIPs in %.1fs", len(rows), elapsed)
    return {row[0]: row[1] for row in rows}


def build_permit_features(conn, snapshot_date, lookback_months=24):
    """
    Build permit-based features for each active ZIP at a given snapshot date.

    For efficiency against 744M records, we use a single query with conditional
    aggregation rather than multiple passes.
    """
    logger.info("Building permit features for snapshot %s...", snapshot_date.strftime("%Y-%m-%d"))
    t0 = time.time()

    # Date boundaries
    d30 = f"'{snapshot_date}'::date - interval '30 days'"
    d90 = f"'{snapshot_date}'::date - interval '90 days'"
    d365 = f"'{snapshot_date}'::date - interval '365 days'"
    d2y = f"'{snapshot_date}'::date - interval '2 years'"

    # Same quarter last year boundaries
    sq_start = f"'{snapshot_date}'::date - interval '1 year' - interval '45 days'"
    sq_end = f"'{snapshot_date}'::date - interval '1 year' + interval '45 days'"

    query = f"""
        SELECT
            zip_code AS zip,
            -- Recent activity
            COUNT(*) FILTER (WHERE date_created >= {d30} AND date_created < '{snapshot_date}'::date) AS permit_count_30d,
            COUNT(*) FILTER (WHERE date_created >= {d90} AND date_created < '{snapshot_date}'::date) AS permit_count_90d,
            COUNT(*) FILTER (WHERE date_created >= {d365} AND date_created < '{snapshot_date}'::date) AS permit_count_365d,

            -- YoY acceleration: this quarter vs same quarter last year
            COUNT(*) FILTER (WHERE date_created >= {d90} AND date_created < '{snapshot_date}'::date) AS this_q,
            COUNT(*) FILTER (WHERE date_created >= {sq_start} AND date_created < {sq_end}) AS last_yr_q,

            -- Average monthly rate over 2 years
            COUNT(*) FILTER (WHERE date_created >= {d2y} AND date_created < '{snapshot_date}'::date) AS total_2y,

            -- Seasonality: current month vs overall average
            COUNT(*) FILTER (
                WHERE EXTRACT(MONTH FROM date_created) = EXTRACT(MONTH FROM '{snapshot_date}'::date)
                AND date_created >= {d2y} AND date_created < '{snapshot_date}'::date
            ) AS same_month_count,

            -- Residential percentage
            COUNT(*) FILTER (
                WHERE project_type ILIKE '%%residential%%'
                AND date_created >= {d365} AND date_created < '{snapshot_date}'::date
            ) AS residential_count

        FROM permits
        WHERE zip_code IS NOT NULL AND zip_code != ''
          AND date_created >= {d2y}
          AND date_created < '{snapshot_date}'::date
        GROUP BY zip_code
        HAVING COUNT(*) >= 10
    """

    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=columns)
    elapsed = time.time() - t0
    logger.info("Built permit features for %d ZIPs in %.1fs", len(df), elapsed)

    if df.empty:
        return df

    # Compute derived features
    months_2y = 24.0
    df["avg_monthly_permits"] = df["total_2y"] / months_2y

    # YoY acceleration (ratio, capped)
    df["yoy_acceleration"] = np.where(
        df["last_yr_q"] > 0,
        df["this_q"] / df["last_yr_q"],
        np.where(df["this_q"] > 0, 2.0, 1.0),
    )
    df["yoy_acceleration"] = df["yoy_acceleration"].clip(0, 5)

    # Seasonality factor
    months_in_2y = 2  # number of times this month appears in 2yr window
    df["seasonality_factor"] = np.where(
        df["avg_monthly_permits"] > 0,
        (df["same_month_count"] / months_in_2y) / df["avg_monthly_permits"],
        1.0,
    )
    df["seasonality_factor"] = df["seasonality_factor"].clip(0, 5)

    # Residential percentage
    df["pct_residential"] = np.where(
        df["permit_count_365d"] > 0,
        df["residential_count"] / df["permit_count_365d"] * 100,
        0,
    )

    # Clean up intermediate columns
    df = df.drop(columns=["total_2y", "this_q", "last_yr_q", "same_month_count", "residential_count"])

    return df


def join_valuation_features(conn, df):
    """Join property valuation features (median sale price, DOM, inventory) by ZIP."""
    if df.empty:
        return df

    logger.info("Joining property valuation features...")
    t0 = time.time()

    zips = tuple(df["zip"].tolist())
    # Chunk if needed (psycopg2 has parameter limits)
    chunk_size = 5000
    all_val_rows = []

    for i in range(0, len(zips), chunk_size):
        chunk = zips[i : i + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        query = f"""
            SELECT DISTINCT ON (zip)
                zip,
                median_sale_price,
                median_dom,
                inventory
            FROM property_valuations
            WHERE zip IN ({placeholders})
            ORDER BY zip, period_end DESC
        """
        with conn.cursor() as cur:
            cur.execute(query, chunk)
            all_val_rows.extend(cur.fetchall())

    if all_val_rows:
        val_df = pd.DataFrame(all_val_rows, columns=["zip", "median_sale_price", "median_dom", "inventory"])

        # Compute YoY price change by getting 1-year-ago price
        for i in range(0, len(zips), chunk_size):
            chunk = zips[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            query_yoy = f"""
                SELECT DISTINCT ON (zip)
                    zip,
                    median_sale_price as price_1y_ago
                FROM property_valuations
                WHERE zip IN ({placeholders})
                  AND period_end <= NOW() - interval '10 months'
                ORDER BY zip, period_end DESC
            """
            with conn.cursor() as cur:
                cur.execute(query_yoy, chunk)
                yoy_rows = cur.fetchall()

        if yoy_rows:
            yoy_df = pd.DataFrame(yoy_rows, columns=["zip", "price_1y_ago"])
            val_df = val_df.merge(yoy_df, on="zip", how="left")
            val_df["price_yoy_change"] = np.where(
                (val_df["price_1y_ago"].notna()) & (val_df["price_1y_ago"] > 0),
                (val_df["median_sale_price"] - val_df["price_1y_ago"]) / val_df["price_1y_ago"] * 100,
                0,
            )
            val_df = val_df.drop(columns=["price_1y_ago"])
        else:
            val_df["price_yoy_change"] = 0

        df = df.merge(val_df, on="zip", how="left")
    else:
        df["median_sale_price"] = np.nan
        df["median_dom"] = np.nan
        df["inventory"] = np.nan
        df["price_yoy_change"] = 0

    elapsed = time.time() - t0
    logger.info("Joined valuation features in %.1fs", elapsed)
    return df


def join_census_features(conn, df):
    """Join census demographics (median income, homeownership rate, median year built)."""
    if df.empty:
        return df

    logger.info("Joining census demographics features...")
    t0 = time.time()

    # Census data is at tract level, not ZIP. We aggregate state-level averages
    # as a proxy, or use a ZIP-to-tract crosswalk if available.
    # For now, use state-level aggregates from census_demographics.
    query = """
        SELECT
            state_fips,
            AVG(median_income) AS median_income,
            AVG(homeownership_rate) AS homeownership_rate,
            AVG(median_year_built) AS median_year_built
        FROM census_demographics
        WHERE median_income IS NOT NULL
        GROUP BY state_fips
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    if rows:
        census_df = pd.DataFrame(rows, columns=["state_fips", "median_income", "homeownership_rate", "median_year_built"])

        # Get ZIP-to-state mapping from permits
        zips = tuple(df["zip"].tolist())
        chunk_size = 5000
        zip_state_rows = []
        for i in range(0, len(zips), chunk_size):
            chunk = zips[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            q = f"""
                SELECT DISTINCT ON (zip_code) zip_code AS zip, state_code AS state
                FROM permits
                WHERE zip_code IN ({placeholders}) AND state_code IS NOT NULL
                ORDER BY zip_code, date_created DESC
            """
            with conn.cursor() as cur:
                cur.execute(q, chunk)
                zip_state_rows.extend(cur.fetchall())

        if zip_state_rows:
            zs_df = pd.DataFrame(zip_state_rows, columns=["zip", "state"])

            # Map state abbreviation to FIPS (simplified — top states)
            state_to_fips = {
                "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
                "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
                "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
                "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
                "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
                "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
                "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
                "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
                "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
                "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56",
                "DC": "11",
            }
            zs_df["state_fips"] = zs_df["state"].map(state_to_fips)
            zs_df = zs_df.merge(census_df, on="state_fips", how="left")
            zs_df = zs_df.drop(columns=["state", "state_fips"])
            df = df.merge(zs_df, on="zip", how="left")
        else:
            df["median_income"] = np.nan
            df["homeownership_rate"] = np.nan
            df["median_year_built"] = np.nan
    else:
        df["median_income"] = np.nan
        df["homeownership_rate"] = np.nan
        df["median_year_built"] = np.nan

    elapsed = time.time() - t0
    logger.info("Joined census features in %.1fs", elapsed)
    return df


def build_labels(conn, df, snapshot_date):
    """
    Build binary labels: did this ZIP have 5+ new permits in the 90 days
    FOLLOWING the snapshot date?
    """
    if df.empty:
        return df

    logger.info("Building labels for snapshot %s...", snapshot_date.strftime("%Y-%m-%d"))
    t0 = time.time()

    future_start = snapshot_date.strftime("%Y-%m-%d")
    future_end_dt = snapshot_date + pd.Timedelta(days=90)
    future_end = future_end_dt.strftime("%Y-%m-%d")

    zips = tuple(df["zip"].tolist())
    chunk_size = 5000
    label_rows = []

    for i in range(0, len(zips), chunk_size):
        chunk = zips[i : i + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        query = f"""
            SELECT zip_code AS zip, COUNT(*) AS future_permits
            FROM permits
            WHERE zip_code IN ({placeholders})
              AND date_created >= '{future_start}'::date
              AND date_created < '{future_end}'::date
            GROUP BY zip_code
        """
        with conn.cursor() as cur:
            cur.execute(query, chunk)
            label_rows.extend(cur.fetchall())

    if label_rows:
        label_df = pd.DataFrame(label_rows, columns=["zip", "future_permits"])
        df = df.merge(label_df, on="zip", how="left")
        df["future_permits"] = df["future_permits"].fillna(0).astype(int)
    else:
        df["future_permits"] = 0

    df["label"] = (df["future_permits"] >= 5).astype(int)

    elapsed = time.time() - t0
    logger.info("Built labels in %.1fs — %d positive (%.1f%%)",
                elapsed, df["label"].sum(), df["label"].mean() * 100)
    return df


def build_training_data(conn, num_snapshots=36):
    """
    Build training data by sliding a monthly window across the last 3 years.
    Creates ~36 observations per ZIP.
    """
    logger.info("Building training data with %d monthly snapshots...", num_snapshots)
    all_frames = []
    now = pd.Timestamp.now()

    for months_ago in range(3, num_snapshots + 3):  # Start 3 months ago (need 90d future)
        snapshot_date = now - pd.DateOffset(months=months_ago)
        snapshot_date = snapshot_date.replace(day=1)  # Normalize to 1st of month

        logger.info("--- Snapshot %d/%d: %s ---", months_ago - 2, num_snapshots,
                     snapshot_date.strftime("%Y-%m-%d"))

        df = build_permit_features(conn, snapshot_date)
        if df.empty:
            continue

        df = join_valuation_features(conn, df)
        df = join_census_features(conn, df)
        df = build_labels(conn, df, snapshot_date)

        df["snapshot_date"] = snapshot_date
        all_frames.append(df)

    if not all_frames:
        logger.error("No training data built!")
        return pd.DataFrame()

    training_df = pd.concat(all_frames, ignore_index=True)
    logger.info("Total training samples: %d (%.1f%% positive)",
                len(training_df), training_df["label"].mean() * 100)
    return training_df


def train_model(training_df):
    """Train XGBoost binary classifier with 80/20 time-based split."""
    logger.info("Training XGBoost model...")

    feature_cols = [c for c in FEATURE_NAMES if c in training_df.columns]
    missing = set(FEATURE_NAMES) - set(feature_cols)
    if missing:
        logger.warning("Missing features (will be filled with 0): %s", missing)
        for col in missing:
            training_df[col] = 0
        feature_cols = FEATURE_NAMES

    # Fill NaN with 0 for features
    X = training_df[feature_cols].fillna(0).values
    y = training_df["label"].values

    # Time-based split: train on older data, test on recent
    snapshot_dates = training_df["snapshot_date"].sort_values().unique()
    cutoff_idx = int(len(snapshot_dates) * 0.8)
    cutoff_date = snapshot_dates[cutoff_idx]

    train_mask = training_df["snapshot_date"] < cutoff_date
    test_mask = training_df["snapshot_date"] >= cutoff_date

    X_train, y_train = X[train_mask], y[train_mask]
    X_test, y_test = X[test_mask], y[test_mask]

    logger.info("Train: %d samples (%.1f%% positive)", len(X_train), y_train.mean() * 100)
    logger.info("Test:  %d samples (%.1f%% positive)", len(X_test), y_test.mean() * 100)

    model = XGBClassifier(
        max_depth=6,
        n_estimators=200,
        learning_rate=0.1,
        objective="binary:logistic",
        eval_metric="auc",
        n_jobs=-1,
        random_state=42,
        use_label_encoder=False,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=20,
    )

    # Evaluate
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred_proba) if len(set(y_test)) > 1 else 0
    precision, recall, f1, _ = precision_recall_fscore_support(y_test, y_pred, average="binary")

    logger.info("=" * 60)
    logger.info("MODEL EVALUATION")
    logger.info("=" * 60)
    logger.info("Accuracy:  %.4f", accuracy)
    logger.info("AUC:       %.4f", auc)
    logger.info("Precision: %.4f", precision)
    logger.info("Recall:    %.4f", recall)
    logger.info("F1:        %.4f", f1)
    logger.info("\n%s", classification_report(y_test, y_pred, target_names=["<5 permits", "5+ permits"]))

    # Feature importance
    importance = model.feature_importances_
    feat_imp = sorted(zip(feature_cols, importance), key=lambda x: x[1], reverse=True)
    logger.info("Feature Importance:")
    for name, imp in feat_imp:
        logger.info("  %-25s %.4f", name, imp)

    # Save model
    model_data = {
        "model": model,
        "feature_names": feature_cols,
        "version": MODEL_VERSION,
        "metrics": {
            "accuracy": float(accuracy),
            "auc": float(auc),
            "precision": float(precision),
            "recall": float(recall),
            "f1": float(f1),
        },
        "feature_importance": {name: float(imp) for name, imp in feat_imp},
        "trained_at": datetime.now(timezone.utc).isoformat(),
    }
    joblib.dump(model_data, MODEL_OUTPUT_PATH)
    logger.info("Model saved to %s", MODEL_OUTPUT_PATH)

    return model, feature_cols, model_data["metrics"], model_data["feature_importance"]


def generate_risk_factors(row, feature_importance):
    """Generate human-readable risk factors from feature values."""
    factors = []

    if row.get("permit_count_30d", 0) > 10:
        factors.append("High recent permit velocity (30d)")
    if row.get("permit_count_90d", 0) > 30:
        factors.append("Strong 90-day permit activity")
    if row.get("yoy_acceleration", 1) > 1.3:
        factors.append("Year-over-year acceleration")
    if row.get("price_yoy_change", 0) > 5:
        factors.append("Rising home prices")
    if row.get("median_sale_price", 0) > 500000:
        factors.append("High-value market")
    if row.get("inventory", 0) < 100:
        factors.append("Low housing inventory")
    if row.get("median_dom", 999) < 30:
        factors.append("Fast-selling market")
    if row.get("seasonality_factor", 1) > 1.2:
        factors.append("Above-average seasonal activity")
    if row.get("pct_residential", 0) > 70:
        factors.append("Predominantly residential permits")
    if row.get("median_year_built", 2000) < 1980:
        factors.append("Aging housing stock (renovation demand)")

    # If no specific factors triggered, add generic ones based on top features
    if not factors:
        factors.append("Consistent permit activity pattern")

    return factors[:5]  # Cap at 5 factors


def batch_score_and_write(conn, model, feature_cols, feature_importance):
    """Score all active ZIPs with latest features and write to permit_predictions."""
    logger.info("Batch scoring all active ZIPs...")
    t0 = time.time()

    now = pd.Timestamp.now()
    snapshot_date = now.replace(day=1)

    # Build current features
    df = build_permit_features(conn, snapshot_date)
    if df.empty:
        logger.error("No features built for current snapshot!")
        return

    df = join_valuation_features(conn, df)
    df = join_census_features(conn, df)

    # Get state for each ZIP
    zips = tuple(df["zip"].tolist())
    chunk_size = 5000
    state_rows = []
    for i in range(0, len(zips), chunk_size):
        chunk = zips[i : i + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        q = f"""
            SELECT DISTINCT ON (zip_code) zip_code AS zip, state_code AS state
            FROM permits
            WHERE zip_code IN ({placeholders}) AND state_code IS NOT NULL
            ORDER BY zip_code, date_created DESC
        """
        with conn.cursor() as cur:
            cur.execute(q, chunk)
            state_rows.extend(cur.fetchall())

    state_map = {r[0]: r[1] for r in state_rows}

    # Prepare features and predict
    for col in feature_cols:
        if col not in df.columns:
            df[col] = 0

    X = df[feature_cols].fillna(0).values
    probabilities = model.predict_proba(X)[:, 1]
    scores = (probabilities * 100).round(1)

    # Estimate predicted permit counts (simple: scale by historical rate)
    avg_90d = df["permit_count_90d"].fillna(0).values
    predicted_permits = np.where(
        probabilities > 0.5,
        np.maximum(avg_90d * (1 + (probabilities - 0.5)), 5),
        avg_90d * probabilities * 2,
    ).astype(int)

    scored_at = datetime.now(timezone.utc)

    # Build rows for insertion
    insert_rows = []
    for idx in range(len(df)):
        zip_code = df.iloc[idx]["zip"]
        row_dict = df.iloc[idx].to_dict()

        features_json = {col: float(row_dict.get(col, 0)) if pd.notna(row_dict.get(col, 0)) else 0
                         for col in feature_cols}
        risk_factors = generate_risk_factors(row_dict, feature_importance)

        insert_rows.append((
            str(uuid.uuid4()),
            zip_code,
            state_map.get(zip_code),
            float(scores[idx]),
            int(predicted_permits[idx]),
            float(probabilities[idx]),
            json.dumps(features_json),
            json.dumps(risk_factors),
            MODEL_VERSION,
            scored_at,
        ))

    # TRUNCATE + INSERT for fresh scores
    logger.info("Writing %d predictions to permit_predictions...", len(insert_rows))
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE permit_predictions")
        execute_values(
            cur,
            """
            INSERT INTO permit_predictions
                (id, zip, state, prediction_score, predicted_permits, confidence,
                 features, risk_factors, model_version, scored_at)
            VALUES %s
            """,
            insert_rows,
            page_size=1000,
        )
    conn.commit()

    elapsed = time.time() - t0
    logger.info("Batch scoring complete: %d ZIPs scored in %.1fs", len(insert_rows), elapsed)
    logger.info("Top 10 predicted ZIPs:")
    top_indices = np.argsort(scores)[::-1][:10]
    for rank, idx in enumerate(top_indices, 1):
        zip_code = df.iloc[idx]["zip"]
        logger.info("  #%d  ZIP %s (%s) — score: %.1f, predicted: %d permits",
                     rank, zip_code, state_map.get(zip_code, "??"),
                     scores[idx], predicted_permits[idx])


def ensure_table_exists(conn):
    """Create permit_predictions table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS permit_predictions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                zip VARCHAR(10) NOT NULL,
                state VARCHAR(2),
                prediction_score FLOAT,
                predicted_permits INTEGER,
                confidence FLOAT,
                features JSONB,
                risk_factors JSONB,
                model_version VARCHAR(50),
                scored_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_predictions_zip ON permit_predictions (zip)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_predictions_state_score ON permit_predictions (state, prediction_score DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_predictions_scored_at ON permit_predictions (scored_at)")
    conn.commit()
    logger.info("permit_predictions table ensured")


def main():
    parser = argparse.ArgumentParser(description="Train predictive permit analytics model")
    parser.add_argument("--db-host", default="100.122.216.15", help="PostgreSQL host (default: T430)")
    parser.add_argument("--db-port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--db-name", default="permits", help="Database name")
    parser.add_argument("--db-user", default="will", help="Database user")
    parser.add_argument("--db-password", default="", help="Database password")
    parser.add_argument("--snapshots", type=int, default=36, help="Number of monthly snapshots for training")
    parser.add_argument("--score-only", action="store_true", help="Skip training, load model and score only")
    args = parser.parse_args()

    total_t0 = time.time()

    conn = get_connection(args)
    ensure_table_exists(conn)

    if args.score_only:
        logger.info("Score-only mode — loading existing model...")
        model_data = joblib.load(MODEL_OUTPUT_PATH)
        model = model_data["model"]
        feature_cols = model_data["feature_names"]
        feature_importance = model_data["feature_importance"]
    else:
        # Build training data
        training_df = build_training_data(conn, num_snapshots=args.snapshots)
        if training_df.empty:
            logger.error("No training data — cannot train model")
            conn.close()
            sys.exit(1)

        # Train model
        model, feature_cols, metrics, feature_importance = train_model(training_df)

    # Batch score all active ZIPs
    batch_score_and_write(conn, model, feature_cols, feature_importance)

    conn.close()
    total_elapsed = time.time() - total_t0
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE — Total time: %.1f minutes", total_elapsed / 60)
    logger.info("Model: %s", MODEL_VERSION)
    logger.info("Saved to: %s", MODEL_OUTPUT_PATH)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
