# Hospital Patient Record Analysis

Big data pipeline that ingests, cleans, and analyzes real New York State
hospital inpatient discharge records using **HDFS** for distributed storage
and **PySpark** for processing, then presents the results in a **custom live
dashboard** built with plain HTML, CSS and JavaScript. It surfaces trends
across diagnoses, illness severity, admission type, age, gender, county, and
billing.

*Big Data Processing, Semester S5. Guide: Ms. Jithika M.*

---

## Architecture

```
Raw CSV (NY SPARCS Hospital Inpatient Discharge, 2010, ~2.5M records)
        │
        ▼
 Random sampling         ← sample_dataset.py (pandas, chunked 15% sample)
        │
        ▼
   HDFS Storage          ← distributed file storage layer
        │
        ▼
  PySpark Cleaning       ← column selection, money/LOS parsing, invalid row removal, dedup
        │
        ▼
  PySpark Analysis       ← groupBy aggregations: diagnosis, severity, admission type, age, gender, county
        │
        ├──► outputs/charts/    ← static charts (matplotlib / seaborn)
        │
        └──► dashboard/data/    ← CSV + meta.json exports
                    │
                    ▼
            Live Dashboard       ← dashboard.html loads the CSVs with fetch()
```

## Tech Stack

| Layer | Tool |
|---|---|
| Distributed storage | Hadoop HDFS 3.3.6 |
| Processing / analysis | PySpark 3.5 |
| Sampling | Pandas (chunked read) |
| Static charts | Matplotlib, Seaborn |
| Dashboard | HTML, CSS, vanilla JavaScript (inline SVG charts, no chart library) |
| Development | VS Code (plain Python scripts) |
| Language | Python 3.11 |

## Dataset

- **Source:** New York State Statewide Planning and Research Cooperative
  System (SPARCS), Hospital Inpatient Discharge data, 2010. A real,
  de-identified government dataset (not synthetic), published under NY
  State's open data program.
- **Size:** the full file has about 2.5 million records (~983 MB). A random
  sample is taken with `sample_dataset.py`, and after cleaning the analysis
  runs on **382,522 records**.
- **Fields used (16):** Health Service Area, Hospital County, Facility Name,
  Age Group, Gender, Race, Length of Stay, Type of Admission, Patient
  Disposition, Discharge Year, CCS Diagnosis Description, APR Severity of
  Illness Description, APR Risk of Mortality, APR Medical Surgical
  Description, Total Charges, Total Costs
- **Why this dataset:** unlike synthetic healthcare datasets (which assign
  cost/outcome values randomly), SPARCS reflects real clinical and billing
  patterns. For example, higher illness severity genuinely correlates with
  higher charges and longer stays, so the analysis produces meaningful,
  defensible findings rather than artifacts of random data generation.

## Project Structure

```
healthcare-bigdata-analysis/
├── data/raw/                     # original CSV (not committed, see below)
├── src/
│   ├── data_cleaning.py          # reusable cleaning functions
│   └── analysis.py               # reusable analysis functions
├── sample_dataset.py             # samples the full CSV down to a workable size
├── run_01_cleaning.py            # loads raw data from HDFS, cleans, saves back to HDFS
├── run_02_analysis.py            # loads clean data, runs analysis, saves charts + dashboard CSVs
├── dashboard/
│   ├── dashboard.html            # live dashboard (5 pages)
│   └── data/                     # CSVs + meta.json written by run_02_analysis.py
├── outputs/charts/               # exported chart images
├── report/                       # final written report
├── requirements.txt
└── README.md
```

> **Note on data:** the raw CSV is not committed to this repo (it is far
> larger than GitHub's 100 MB file limit). During processing it is stored in
> HDFS at `/healthcare_project/raw/hospital_discharge.csv`. Get the dataset
> from Kaggle ("New York State Hospital Inpatient Discharge", 2010) or NY's
> official open data portal (health.data.ny.gov) to reproduce.

## Setup & Reproduction

1. **Python environment (Python 3.11 recommended for PySpark compatibility):**
   ```
   pip install -r requirements.txt
   ```
2. **Sample the raw data** (optional, only if you start from the full file).
   Put the full file at `data/raw/hospital_discharge.csv`, then run:
   ```
   python sample_dataset.py
   ```
   This writes `data/raw/hospital_discharge_sample.csv`.
3. **Hadoop (HDFS)**: install Hadoop 3.3.6 and start the services:
   ```
   start-dfs.cmd
   start-yarn.cmd
   ```
4. **Load the data into HDFS.** `run_01_cleaning.py` reads
   `/healthcare_project/raw/hospital_discharge.csv`, so upload the file you
   want to analyze under that name:
   ```
   hdfs dfs -mkdir -p /healthcare_project/raw
   hdfs dfs -put data/raw/hospital_discharge_sample.csv /healthcare_project/raw/hospital_discharge.csv
   ```
5. **Run the pipeline in order:**
   ```
   python run_01_cleaning.py
   python run_02_analysis.py
   ```
   `run_02_analysis.py` saves the charts to `outputs/charts/` and writes the
   dashboard's data files (7 CSVs + `meta.json`) to `dashboard/data/`.

## Live Dashboard

The dashboard replaces a traditional BI tool. It is a single self-contained
`dashboard.html` that loads the CSV files exported by the pipeline using
`fetch()`, so every number on screen comes straight from the PySpark output
and nothing is typed in by hand.

**Pages:** Overview, Diagnoses, Severity & Stay, Patients, Method.
**Features:** headline KPI cards, search/filter on diagnoses, hover details on
charts, and chart/table toggles. Charts are drawn as inline SVG with no
external charting library.

**To run it:** browsers block `fetch()` on local files, so the dashboard has to
be served over HTTP (double-clicking the HTML file will not work):

```
cd dashboard
python -m http.server 8000
```

Then open <http://localhost:8000/dashboard.html>. Re-run
`run_02_analysis.py` any time to refresh the data the dashboard shows.

## Key Findings

Based on 382,522 cleaned inpatient discharge records (NY State SPARCS, 2010).

**1. Most common diagnoses**
Liveborn (newborn deliveries) is the largest category with 27,151 discharges,
about 7% of all records. It is followed by Mood Disorders (9,262), Congestive
Heart Failure (9,250), Septicemia (9,132) and Chest Pain (8,944).

**2. Illness severity drives cost**
Average total charges rise sharply with severity:

| Severity | Avg. Total Charges |
|---|---|
| Minor | $16,510 |
| Moderate | $24,353 |
| Major | $40,910 |
| Extreme | $110,531 |

Extreme-severity cases are billed about **6.7×** more than Minor cases.

**3. Severity and admission type drive length of stay**
Extreme-severity patients stay **16.4 days** on average versus **3.2 days** for
Minor (about 5.1× longer). By admission type, Trauma has the longest average
stay (7.3 days), followed by Urgent (6.15) and Emergency (5.84). Newborn
admissions have the shortest (4.31).

**4. Who the patients are**
The 70+ age group has the most discharges (108,321, about 28% of the total),
followed by ages 50–69 (101,768). Patients aged 50 and over make up roughly
55% of all discharges. Gender splits vary by condition: for example,
Coronary Atherosclerosis discharges are mostly male (5,815 male vs 3,093
female).

**5. Where patients are treated**
Manhattan has the highest volume (65,501 discharges, about 17%), followed by
Kings (41,684), Queens (31,805), Nassau (31,603) and Bronx (30,172).

## Limitations

- Results come from a sample of the full SPARCS file, not the complete dataset.
- Some records have a missing severity value; these are left out of the
  severity comparisons.
- Stays coded "120+" are capped at 120 days.
- Charges are billed amounts, not the actual cost of care.
- Findings show associations, not causation.

## Future Improvements

- Scale to a multi-node Hadoop cluster for true distributed processing
- Add a Spark ML model to predict total charges or length of stay from
  patient/admission features
- Repeat the severity analysis using Total Costs instead of Total Charges
- Filter out records with a missing severity value during cleaning
- Extend to multiple discharge years to analyze trends over time

## Team

Adhithya Rajesh, Anannya AK, Anuvarnna A, Mayookha K
Guide: Ms. Jithika M
