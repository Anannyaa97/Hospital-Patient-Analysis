"""
Analysis functions for the Hospital Patient Record Analysis project.
Built for the NY State SPARCS Hospital Inpatient Discharge dataset.

Each function takes the cleaned PySpark DataFrame and returns a small
aggregated DataFrame suitable for plotting.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def top_conditions(df: DataFrame, n: int = 10) -> DataFrame:
    """Top N most common diagnoses (CCS Diagnosis Description) by patient count."""
    return (
        df.groupBy("CCS Diagnosis Description")
          .count()
          .orderBy(F.desc("count"))
          .limit(n)
    )


def avg_charges_by_severity(df: DataFrame) -> DataFrame:
    """Average total charges by APR Severity of Illness level."""
    return (
        df.groupBy("APR Severity of Illness Description")
          .agg(F.round(F.avg("Total Charges"), 2).alias("Avg Charges"))
          .orderBy(F.desc("Avg Charges"))
    )


def avg_stay_by_admission_type(df: DataFrame) -> DataFrame:
    """Average length of stay per admission type (Urgent/Emergency/Elective/etc.)."""
    return (
        df.groupBy("Type of Admission")
          .agg(F.round(F.avg("Length of Stay"), 2).alias("Avg Length of Stay"))
          .orderBy(F.desc("Avg Length of Stay"))
    )


def age_group_distribution(df: DataFrame) -> DataFrame:
    """Patient count per age group (dataset already provides age ranges)."""
    return (
        df.groupBy("Age Group")
          .count()
          .orderBy("Age Group")
    )


def avg_stay_by_severity(df: DataFrame) -> DataFrame:
    """Average length of stay by illness severity - tests a real clinical relationship."""
    return (
        df.groupBy("APR Severity of Illness Description")
          .agg(F.round(F.avg("Length of Stay"), 2).alias("Avg Length of Stay"))
          .orderBy(F.desc("Avg Length of Stay"))
    )


def gender_distribution_top_conditions(df: DataFrame, n: int = 6) -> DataFrame:
    """Gender split across the N most common diagnoses."""
    top = [row["CCS Diagnosis Description"] for row in top_conditions(df, n).collect()]
    return (
        df.filter(F.col("CCS Diagnosis Description").isin(top))
          .groupBy("CCS Diagnosis Description", "Gender")
          .count()
          .orderBy("CCS Diagnosis Description", "Gender")
    )


def top_counties_by_volume(df: DataFrame, n: int = 10) -> DataFrame:
    """Top N hospital counties by number of discharges (patient volume)."""
    return (
        df.groupBy("Hospital County")
          .count()
          .orderBy(F.desc("count"))
          .limit(n)
    )

