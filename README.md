# SFTP Encrypted CSV Ingestion with Lakeflow Spark Declarative Pipelines

## About

This project demonstrates how to use the **Databricks SFTP connector** together with **Lakeflow Spark Declarative Pipelines** to ingest **GPG-encrypted CSV files** from an SFTP server into Unity Catalog tables.

The pipeline is implemented in `my_transformation.py` and uses a classic **bronze → silver → gold** pattern:

- **Bronze** – Incrementally ingest encrypted files from SFTP as binary.
- **Silver** – Decrypt the binary payload into CSV text using a PGP private key stored in Databricks Secrets.
- **Gold** – Parse the CSV into a typed Delta table ready for analytics.

## How to Use

There are a couple of things that you need to do run the pipeline:
- The SFTP Path must be specified to match the proper connecting location
- The private_key_blob and passphrase must be replaced with the names you chose for UC Secrets
- In the settings page of the pipeline, under Pipeline Environment, you must add python-gnupg==0.5.3 as a dependency

## Resources
- https://learn.microsoft.com/en-us/azure/databricks/ingestion/sftp
- https://learn.microsoft.com/en-us/azure/databricks/security/secrets/
