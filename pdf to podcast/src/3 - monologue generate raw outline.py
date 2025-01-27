# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %run "/Workspace/nvidia blueprints/workflow/shared/monologue prompts"

# COMMAND ----------

# MAGIC %run "/Workspace/nvidia blueprints/workflow/shared/utils"

# COMMAND ----------

monologue_summary_table = "llm_agents_catalog.pdf_to_podcast.monologue_summary"
raw_outline_output_table = "llm_agents_catalog.pdf_to_podcast.monologue_raw_outline"

# COMMAND ----------

raw_outlines_df = spark.table(monologue_summary_table)
raw_outlines = raw_outlines_df.collect()

# COMMAND ----------

# Format documents as a string list for consistency with podcast flow
documents = [f"Document: {pdf['filename']}\n{pdf['monologue_summarize_pdf']}" for pdf in raw_outlines]

template = FinancialSummaryPrompts.get_template(
  "monologue_multi_doc_synthesis_prompt"
  )
  
prompt = template.render(
        focus_instructions=None,
        documents="\n\n".join(documents),
    )

raw_outline = score_model(prompt, "https://adb-984752964297111.11.azuredatabricks.net", "databricks-meta-llama-3-1-405b-instruct", True).json()["choices"][0]["message"]["content"]

# COMMAND ----------

file_list = [row["filename"] for row in raw_outlines_df.select("filename").collect()]

data = [Row(files=file_list, raw_outline=raw_outline)]
spark.createDataFrame(data).write.mode("overwrite").saveAsTable(raw_outline_output_table)
