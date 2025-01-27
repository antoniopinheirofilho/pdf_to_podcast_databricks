# Databricks notebook source
# MAGIC %pip install docling

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

files_volume = "/Volumes/llm_agents_catalog/pdf_to_podcast/pdfs_realty_income_test"
output_table = "llm_agents_catalog.pdf_to_podcast.markdowns"

# COMMAND ----------

# Create a list of file paths
files = [file_info.path.replace("dbfs:", "") for file_info in dbutils.fs.ls(files_volume)]

# COMMAND ----------

from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import ConversionStatus
import os

# COMMAND ----------

converter = DocumentConverter()

conversion_results = converter.convert_all(
            files,
            raises_on_error=True,
        )

markdown_results = []

for result in conversion_results:
  file_path = str(result.input.file)

  if result.status in {
      ConversionStatus.SUCCESS,
      ConversionStatus.PARTIAL_SUCCESS,
  }:
      markdown = result.document.export_to_markdown()
      markdown_results.append(
          {
              "filename": os.path.basename(file_path),
              "markdown": markdown,
          }
      )
  else:
      raise Exception(f"Conversion failed for {file_path}")

# COMMAND ----------

print(markdown_results)

# COMMAND ----------

# Convert the list of dictionaries to a Spark DataFrame
df = spark.createDataFrame(markdown_results)

# Write output to a delta table
df.write.format("delta").mode("overwrite").saveAsTable(output_table)
