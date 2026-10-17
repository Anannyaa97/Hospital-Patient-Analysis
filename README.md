# Hospital Patient Record Analysis

Big data pipeline that ingests, cleans, and analyzes real New York State
hospital inpatient discharge records using **HDFS** for distributed storage
and **PySpark** for processing — surfacing trends across diagnoses, illness
severity, admission type, age, gender, and billing.

---

## Architecture

```
Raw CSV (NY SPARCS Hospital Inpatient Discharge, 2010)
        │
        ▼
   HDFS Storage        ← distributed file storage layer
        │
        ▼
  PySpark Cleaning      ← column selection, money/LOS parsing, invalid row removal, dedup
        │
        ▼
  PySpark Analysis      ← groupby aggregations: diagnosis, severity, admission type, age, gender, county
        │
        ▼
   Visualization        ← matplotlib / seaborn charts
        │
        ▼
      Report            ← findings + recommendations
```

## Tech Stack

| Layer | Tool |
|---|---|
| Distributed storage | Hadoop HDFS 3.3.6 |
| Processing / analysis | PySpark 3.5 |
| Visualization | Matplotlib, Seaborn |
| Development | VS Code (plain Python scripts) |
| Language | Python 3.11 |

## Dataset

- **Source:** New York State Statewide Planning and Research Cooperative
  System (SPARCS) — Hospital Inpatient Discharge data, 2010. A real,
  de-identified government dataset (not synthetic), published under NY
  State's open data program.
- **Fields used:** Health Service Area, Hospital County, Facility Name, Age
  Group, Gender, Race, Length of Stay, Type of Admission, Patient
  Disposition, CCS Diagnosis Description, APR Severity of Illness
  Description, APR Risk of Mortality, Total Charges, Total Costs
- **Why this dataset:** unlike synthetic healthcare datasets (which assign
  cost/outcome values randomly), SPARCS reflects real clinical and billing
  patterns — e.g. higher illness severity genuinely correlates with higher
  charges and longer stays — so the analysis produces meaningful, defensible
  findings rather than artifacts of random data generation.

## Project Structure

```
healthcare-bigdata-analysis/
├── data/raw/                     # original CSV (not committed — see below)
├── src/
│   ├── data_cleaning.py          # reusable cleaning functions
│   └── analysis.py               # reusable analysis functions
├── run_01_cleaning.py            # loads raw data from HDFS, cleans, saves back to HDFS
├── run_02_analysis.py            # loads clean data, runs analysis, saves charts
├── outputs/charts/                # exported chart images
├── report/                        # final written report
├── requirements.txt
└── README.md
```

> **Note on data:** the raw CSV is not committed to this repo (standard
> practice for datasets). It's stored in HDFS at
> `/healthcare_project/raw/hospital_discharge.csv` during processing.
> Get the dataset from Kaggle ("New York State Hospital Inpatient Discharge",
> 2010) or NY's official open data portal (health.data.ny.gov) to reproduce.

## Setup & Reproduction

1. **Hadoop (HDFS)** — install Hadoop 3.3.6, start services:
   ```
   start-dfs.cmd
   start-yarn.cmd
   ```
2. **Load data into HDFS:**
   ```
   hdfs dfs -mkdir -p /healthcare_project/raw
   hdfs dfs -put data/raw/hospital_discharge.csv /healthcare_project/raw/
   ```
3. **Python environment (Python 3.11 recommended for PySpark compatibility):**
   ```
   pip install -r requirements.txt
   ```
4. **Run the pipeline in order:**
   ```
   python run_01_cleaning.py
   python run_02_analysis.py
   ```

## Key Findings

_(fill in after running the analysis — e.g. most common diagnosis, how
average charges scale with illness severity, which admission type has the
longest average stay, age group distribution, county-level patient volume)_

## Future Improvements

- Scale to a multi-node Hadoop cluster for true distributed processing
- Add a Spark ML model to predict total charges or length of stay from
  patient/admission features
- Build a live dashboard (Streamlit / Power BI) on top of the analysis output
- Extend to multiple discharge years to analyze trends over time

