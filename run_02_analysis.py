"""
Step 2: Load cleaned SPARCS hospital discharge data from HDFS, run analysis,
save charts as PNGs, and export the same results as CSVs for the dashboard.

Run with:
    python run_02_analysis.py

Charts are saved into outputs/charts/
CSV data for the dashboard is saved into dashboard/data/
"""

import os
import json
from pyspark.sql import SparkSession
import matplotlib
matplotlib.use("Agg")  # save charts to file without needing a display
import matplotlib.pyplot as plt
import seaborn as sns

from src.analysis import (
    top_conditions, avg_charges_by_severity, avg_stay_by_admission_type,
    age_group_distribution, avg_stay_by_severity, gender_distribution_top_conditions,
    top_counties_by_volume
)

HDFS_CLEAN_PATH = "hdfs://localhost:9000/healthcare_project/clean/hospital_discharge_clean"
CHARTS_DIR = "outputs/charts"
DATA_DIR = "dashboard/data"

sns.set_theme(style="whitegrid")
PALETTE = ["#2C6E91", "#4E9C81", "#E4A11B", "#C1443C", "#7B5EA7", "#5D8AA8"]


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("HospitalDischargeAnalysis")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    print("\n=== Loading cleaned data from HDFS ===")
    clean_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(HDFS_CLEAN_PATH)
    )
    clean_df.cache()
    total_rows = clean_df.count()
    print("Row count:", total_rows)

    # Save total row count for the dashboard (used for "% of sample" figures)
    with open(f"{DATA_DIR}/meta.json", "w") as f:
        json.dump({"total_rows": total_rows}, f)
    print(f"Saved {DATA_DIR}/meta.json")

    print("\n=== Chart 1: Top 10 diagnoses ===")
    pdf = top_conditions(clean_df, 10).toPandas()
    pdf.to_csv(f"{DATA_DIR}/top_conditions.csv", index=False)
    print(f"Saved {DATA_DIR}/top_conditions.csv")
    plt.figure(figsize=(9, 6))
    sns.barplot(data=pdf, x="count", y="CCS Diagnosis Description", palette=PALETTE * 2)
    plt.title("Top 10 Diagnoses by Patient Count", fontsize=14, fontweight="bold")
    plt.xlabel("Number of Patients")
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/top_conditions.png", dpi=150)
    plt.close()
    print("Saved top_conditions.png")

    print("\n=== Chart 2: Average charges by severity ===")
    pdf = avg_charges_by_severity(clean_df).toPandas()
    pdf.to_csv(f"{DATA_DIR}/avg_charges_by_severity.csv", index=False)
    print(f"Saved {DATA_DIR}/avg_charges_by_severity.csv")
    plt.figure(figsize=(8, 5))
    sns.barplot(data=pdf, x="Avg Charges", y="APR Severity of Illness Description", palette=PALETTE)
    plt.title("Average Total Charges by Illness Severity", fontsize=14, fontweight="bold")
    plt.xlabel("Average Total Charges ($)")
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/avg_charges_by_severity.png", dpi=150)
    plt.close()
    print("Saved avg_charges_by_severity.png")

    print("\n=== Chart 3: Average length of stay by admission type ===")
    pdf = avg_stay_by_admission_type(clean_df).toPandas()
    pdf.to_csv(f"{DATA_DIR}/avg_stay_by_admission.csv", index=False)
    print(f"Saved {DATA_DIR}/avg_stay_by_admission.csv")
    plt.figure(figsize=(7, 5))
    sns.barplot(data=pdf, x="Type of Admission", y="Avg Length of Stay", palette=PALETTE)
    plt.title("Average Length of Stay by Admission Type", fontsize=14, fontweight="bold")
    plt.ylabel("Average Days")
    plt.xlabel("")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/avg_stay_by_admission.png", dpi=150)
    plt.close()
    print("Saved avg_stay_by_admission.png")

    print("\n=== Chart 4: Age group distribution ===")
    pdf = age_group_distribution(clean_df).toPandas()
    pdf.to_csv(f"{DATA_DIR}/age_group_distribution.csv", index=False)
    print(f"Saved {DATA_DIR}/age_group_distribution.csv")
    plt.figure(figsize=(7, 5))
    sns.barplot(data=pdf, x="Age Group", y="count", palette=PALETTE)
    plt.title("Patient Count by Age Group", fontsize=14, fontweight="bold")
    plt.ylabel("Number of Patients")
    plt.xlabel("")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/age_group_distribution.png", dpi=150)
    plt.close()
    print("Saved age_group_distribution.png")

    print("\n=== Chart 5: Average length of stay by severity ===")
    pdf = avg_stay_by_severity(clean_df).toPandas()
    pdf.to_csv(f"{DATA_DIR}/avg_stay_by_severity.csv", index=False)
    print(f"Saved {DATA_DIR}/avg_stay_by_severity.csv")
    plt.figure(figsize=(8, 5))
    sns.barplot(data=pdf, x="Avg Length of Stay", y="APR Severity of Illness Description", palette=PALETTE)
    plt.title("Average Length of Stay by Illness Severity", fontsize=14, fontweight="bold")
    plt.xlabel("Average Days")
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/avg_stay_by_severity.png", dpi=150)
    plt.close()
    print("Saved avg_stay_by_severity.png")

    print("\n=== Chart 6: Gender split across top 6 diagnoses ===")
    pdf = gender_distribution_top_conditions(clean_df, 6).toPandas()
    pdf.to_csv(f"{DATA_DIR}/gender_by_condition.csv", index=False)
    print(f"Saved {DATA_DIR}/gender_by_condition.csv")
    pivot = pdf.pivot(index="CCS Diagnosis Description", columns="Gender", values="count").fillna(0)
    pivot.plot(kind="bar", figsize=(9, 6), color=PALETTE[:2])
    plt.title("Gender Distribution Across Top 6 Diagnoses", fontsize=14, fontweight="bold")
    plt.ylabel("Number of Patients")
    plt.xlabel("")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/gender_by_condition.png", dpi=150)
    plt.close()
    print("Saved gender_by_condition.png")

    print("\n=== Chart 7: Top 10 counties by patient volume ===")
    pdf = top_counties_by_volume(clean_df, 10).toPandas()
    pdf.to_csv(f"{DATA_DIR}/top_counties.csv", index=False)
    print(f"Saved {DATA_DIR}/top_counties.csv")
    plt.figure(figsize=(8, 6))
    sns.barplot(data=pdf, x="count", y="Hospital County", palette=PALETTE * 2)
    plt.title("Top 10 Counties by Patient Volume", fontsize=14, fontweight="bold")
    plt.xlabel("Number of Discharges")
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/top_counties.png", dpi=150)
    plt.close()
    print("Saved top_counties.png")

    spark.stop()
    print(f"\nDone. Charts saved in {CHARTS_DIR}/, dashboard data saved in {DATA_DIR}/")


if __name__ == "__main__":
    main()
