"""This pipeline is based on the docs from https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp"""
from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, LongType, BinaryType, DateType, IntegerType, DoubleType
)
import gnupg
from pyspark.sql.functions import col, explode, split, trim, from_csv, udf
import os
import tempfile


# TODO: Modify these variables with your relevant values
# For more information on secrets, see https://learn.microsoft.com/en-us/azure/databricks/security/secrets/ 
SFTP_PATH = "sftp://srikarsblob.testcontainer.srikar@srikarsblob.blob.core.windows.net:22/"
private_key_blob = dbutils.secrets.get("srikartestsecretscope", "pgp_private_key")
passphrase = dbutils.secrets.get("srikartestsecretscope", "pgp_passphrase")


# -------------------- Bronze Layer: Fetch files --------------------
binary_schema = StructType([
    StructField("path", StringType(), True),
    StructField("modificationTime", TimestampType(), True),
    StructField("length", LongType(), True),
    StructField("content", BinaryType(), True),  
])

@dp.table(
    name="sftp_demo_bronze",
    schema=binary_schema,                 
)
def sftp_demo_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .schema(binary_schema)            
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(SFTP_PATH)
    )

# -------------------- Silver Layer: Decrypt files --------------------
_gpg = None

def decrypt_bytes(buf: bytes,
                  key_blob: str = private_key_blob,
                  pw: str = passphrase) -> str:
    """
    buf: encrypted bytes from BinaryType column `content`
    returns: decrypted CSV text
    """
    global _gpg

    if buf is None:
        return None

    if _gpg is None:
        gnupg_home = os.path.join(tempfile.gettempdir(), "gnupg_srikar")
        os.makedirs(gnupg_home, exist_ok=True)

        _gpg = gnupg.GPG(gnupghome=gnupg_home)

        import_result = _gpg.import_keys(key_blob)
        if import_result.count == 0:
            raise Exception(f"Could not import private key: {import_result.stderr}")

    result = _gpg.decrypt(buf, passphrase=pw)

    if not result.ok:
        raise Exception(f"GPG decrypt failed: {result.status} | {result.stderr}")

    return str(result)

decrypt_udf = udf(decrypt_bytes, StringType())


@dp.table(name="sftp_demo_silver")
def sftp_demo_silver():
    bronze_df = spark.readStream.table("sftp_demo_bronze")
    return bronze_df.select(
        "path",
        decrypt_udf(col("content")).alias("csv_text")
    )


# -------------------- Gold Layer: Format into table --------------------
csv_schema_ddl = """
patient_id STRING,
first_name STRING,
last_name STRING,
date_of_birth DATE,
gender STRING,
blood_type STRING,
admission_date DATE,
discharge_date DATE,
diagnosis STRING,
treatment STRING,
attending_physician STRING,
room_number INT,
insurance_provider STRING,
total_charges DOUBLE
"""

@dp.table(name="sftp_demo_gold")  
def sftp_demo_gold():
    silver_df = spark.readStream.table("sftp_demo_silver") 

    lines = (
        silver_df
        .select(
            "path",
            explode(split(col("csv_text"), "\n")).alias("line_raw")
        )
        .select(
            "path",
            trim(col("line_raw")).alias("line")
        )
        .filter(col("line") != "")         
    )

    parsed = lines.select(
        "path",
        from_csv(
            col("line"),
            csv_schema_ddl
        ).alias("data")
    )

    filtered = parsed.filter(col("data.patient_id") != "patient_id")

    return filtered.select(
        "path",
        "data.*"
    )