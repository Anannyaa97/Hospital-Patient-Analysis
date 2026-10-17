"""
Data cleaning functions for the Hospital Patient Record Analysis project.
Built for the NY State SPARCS Hospital Inpatient Discharge dataset (real,
de-identified government data).

Each function takes and returns a PySpark DataFrame, so cleaning steps can be
chained together and reused from any notebook or script.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Columns we actually use for this analysis. The raw file has extra columns
# (license numbers, birth weight, zip code, abortion indicator, etc.) that
# are either mostly blank or not relevant to our trend analysis, so we drop
# them for a cleaner, more focused dataset.
KEEP_COLUMNS = [
    "Health Service Area",
    "Hospital County",
    "Facility Name",
    "Age Group",
    "Gender",
    "Race",
    "Length of Stay",
    "Type of Admission",
    "Patient Disposition",
    "Discharge Year",
    "CCS Diagnosis Description",
    "APR Severity of Illness Description",
    "APR Risk of Mortality",
    "APR Medical Surgical Description",
    "Total Charges",
    "Total Costs",
]


def load_raw_data(spark, hdfs_path: str) -> DataFrame:
    """Load the raw CSV from HDFS with header + inferred schema."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(hdfs_path)
    )


def select_relevant_columns(df: DataFrame) -> DataFrame:
    """Keep only the columns we need for analysis."""
    return df.select(*KEEP_COLUMNS)


def clean_money_columns(df: DataFrame) -> DataFrame:
    """
    Total Charges / Total Costs sometimes arrive as strings with commas or
    stray whitespace (common in NY open-data exports). Strip non-numeric
    characters and cast to double so they can be aggregated.
    """
    for col_name in ["Total Charges", "Total Costs"]:
        df = df.withColumn(
            col_name,
            F.regexp_replace(F.col(col_name).cast("string"), "[^0-9.\\-]", "").cast("double")
        )
    return df


def clean_length_of_stay(df: DataFrame) -> DataFrame:
    """
    Length of Stay is sometimes coded '120 +' for very long stays. Extract
    the numeric part and cast to integer.
    """
    return df.withColumn(
        "Length of Stay",
        F.regexp_extract(F.col("Length of Stay").cast("string"), r"(\d+)", 1).cast("int")
    )


def drop_invalid_rows(df: DataFrame) -> DataFrame:
    """
    Drop rows with missing key fields, or non-positive/negative charges,
    costs, or length of stay - these indicate data entry errors.
    """
    return (
        df.dropna(subset=[
            "Age Group", "Gender", "CCS Diagnosis Description",
            "Length of Stay", "Total Charges", "Total Costs"
        ])
        .filter(F.col("Total Charges") > 0)
        .filter(F.col("Total Costs") > 0)
        .filter(F.col("Length of Stay") > 0)
    )


def drop_duplicates(df: DataFrame) -> DataFrame:
    """Remove fully duplicate rows."""
    return df.dropDuplicates()


def clean_pipeline(spark, hdfs_path: str) -> DataFrame:
    """Run the full cleaning pipeline end to end and return the clean DataFrame."""
    df = load_raw_data(spark, hdfs_path)
    df = select_relevant_columns(df)
    df = clean_money_columns(df)
    df = clean_length_of_stay(df)
    df = drop_invalid_rows(df)
    df = drop_duplicates(df)
    return df

