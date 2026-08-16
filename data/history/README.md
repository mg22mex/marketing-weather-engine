# Committed history archives (see history_archive.py)

- `sales_ledger.csv` — rolling Triple Whale sales archived each pipeline run
- `weather/YYYY-MM-DD/` — daily copies of `marketing_weather_report_*.csv`

Bulk pre-2024-lookback sales come from `data/cleaned_omnichannel_sales.csv`.

Bootstrap once locally:

```bash
python history_archive.py --bootstrap-sales
python history_archive.py --archive-weather
python run_intel_pipeline.py
```
