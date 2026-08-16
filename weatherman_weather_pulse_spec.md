# Weatherman Weather Pulse Specification & Multi-Region Expansion

## 1. Objective
Update the `weatherman_weather_pulse.py` engine to support multi-region weather polling for the US, UK, and Canada using our established target locations (based on historical sales volume and demand hubs). The output should be split into distinct regional files for Looker Studio. Historical orders/revenue fields must be excluded from the generated CSV files.

## 2. Regional Separation and Outputs
The script must generate three separate, region-specific CSV files:
- `marketing_weather_report_us.csv`
- `marketing_weather_report_uk.csv`
- `marketing_weather_report_can.csv`

The exported fields must consist of exactly 9 columns:
`City, State, Lat, Lon, Marketing_Action, Rain_Amount, Yesterday_Rain, Max_UV_Index, Logic_Summary`

## 3. Data Processing & API Endpoints
- **US Execution**: Call NWS for QPF data, and Open-Meteo as a fallback/historical source.
- **UK & Canada Execution**: Utilize Open-Meteo endpoints for precipitation and UV data.
- **Targeted Location Strategy**: Retain the targeted list of high-volume sales hubs and outliers without expanding to all population centers >10k to maintain performance and avoid API throttling.

## 4. Polished Decision Logic
1. **Scale Umbrellas (Residual Demand)**:
   - Triggered if previous-day rain (Open-Meteo archive) is $\ge 0.50 \text{ inches}$.
2. **Scale Umbrellas**:
   - Triggered if forward-looking QPF (from NWS or Open-Meteo model) is $\ge 0.20 \text{ inches}$ or $\ge 0.50 \text{ inches}$ (heavy). Only triggered if the Residual Demand condition does not fire.
3. **Sun Protection**:
   - Triggered when the UV index is $\ge 8.0$.
4. **Baseline**:
   - Default condition when no threshold is met.

## 5. Reliability & Performance
- Maintain the asynchronous, fault-tolerant structure with a retry-delay mechanism (3 retries with exponential backoff) and a timeout of 7 seconds to prevent thread hanging.