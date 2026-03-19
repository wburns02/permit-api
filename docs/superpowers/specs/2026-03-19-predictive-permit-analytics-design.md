# Predictive Permit Analytics — Design Spec

## Purpose

Predict which ZIP codes are likely to see new permit filings in the next 90 days. Uses XGBoost trained on historical permit patterns + market/demographic data. The highest-value premium feature in the platform — worth $200-400K/yr.

## Architecture

```
R730 (training)                    T430 (data)                Railway (serving)
+------------------------+        +------------------+       +-------------------+
| train_predictive.py    | -----> | permits (744M)   |       | FastAPI endpoint   |
| - feature engineering  |        | property_vals    |       | /v1/predictions/*  |
| - XGBoost training     |        | census_demo      |       | reads from         |
| - batch scoring        |        |                  |       | permit_predictions |
| - writes predictions   | -----> | permit_predictions|<---->| table on T430     |
+------------------------+        +------------------+       +-------------------+
```

## Database

### New table: `permit_predictions`

| Column | Type | Description |
|--------|------|-------------|
| id | UUID PK | |
| zip | VARCHAR(10) NOT NULL | ZIP code |
| state | VARCHAR(2) | |
| prediction_score | FLOAT | 0-100, probability of 5+ permits in next 90 days |
| predicted_permits | INTEGER | Expected permit count next 90 days |
| confidence | FLOAT | Model confidence 0-1 |
| features | JSONB | Feature values used for this prediction |
| risk_factors | JSONB | Human-readable factors driving the prediction |
| model_version | VARCHAR(50) | Model version identifier |
| scored_at | TIMESTAMPTZ | When this prediction was computed |

Indexes: (zip), (state, prediction_score DESC), (scored_at)

## Training Pipeline — `scripts/train_predictive_model.py`

Runs on R730 (88 cores, 755GB RAM, direct T430 DB access).

### Feature Engineering (SQL against T430)

For each ZIP with 10+ historical permits, compute:

| Feature | Description | Source |
|---------|-------------|--------|
| permit_count_30d | Permits filed last 30 days | permits |
| permit_count_90d | Permits filed last 90 days | permits |
| permit_count_365d | Permits filed last year | permits |
| yoy_acceleration | This quarter vs same quarter last year | permits |
| avg_monthly_permits | Average monthly rate over 2 years | permits |
| seasonality_factor | Current month vs ZIP's historical monthly average | permits |
| pct_residential | % residential permits | permits |
| avg_valuation | Average permit valuation | permits |
| median_sale_price | Latest median sale price | property_valuations |
| price_yoy_change | YoY price change | property_valuations |
| median_dom | Days on market | property_valuations |
| inventory | Active listings | property_valuations |
| median_income | Area median income | census_demographics |
| homeownership_rate | % homeowners | census_demographics |
| median_year_built | Median home age | census_demographics |

### Label

Binary: did this ZIP have 5+ new permits in the 90 days FOLLOWING the feature snapshot?

Build training data by sliding a window across the last 3 years of data (monthly snapshots), creating ~36 observations per ZIP.

### Model

- XGBoost binary classifier
- Train/test split: 80/20 by time (train on older, test on recent)
- Hyperparameters: max_depth=6, n_estimators=200, learning_rate=0.1
- Output: probability score, scaled to 0-100

### Batch Scoring

After training, score ALL active ZIPs using the latest features. Write results to `permit_predictions` table. Intended to run nightly via cron.

### Output

- Model file: `permit_predict_model.joblib` (saved on R730)
- Metrics: accuracy, AUC, precision/recall logged to stdout
- Feature importance rankings

## API Endpoints — `app/api/v1/predictions.py`

### GET /v1/predictions/zip?zip=78701

Returns prediction for a single ZIP. Reads from `permit_predictions` table.

Response:
```json
{
  "zip": "78701",
  "state": "TX",
  "prediction_score": 87,
  "predicted_permits": 34,
  "confidence": 0.92,
  "risk_factors": ["High recent permit velocity", "Rising home prices", "Young housing stock"],
  "features": { ... },
  "model_version": "v1_2026-03-19",
  "scored_at": "2026-03-19T..."
}
```

Tier: Pro Leads+

### GET /v1/predictions/hotspots?state=TX&limit=50

Top predicted ZIPs ranked by prediction_score. Optional state filter.

Tier: Pro Leads+

### GET /v1/predictions/stats

Public. Model metadata: accuracy, ZIPs scored, last training date, feature importance top 5.

## Implementation Order

1. Add `PermitPrediction` model to `data_layers.py`
2. Create `app/api/v1/predictions.py` with 3 endpoints
3. Register in `main.py`
4. Create `scripts/train_predictive_model.py`
5. Copy to R730, run training
6. Deploy API, run Playwright tests
7. Update pricing page with predictive analytics feature
