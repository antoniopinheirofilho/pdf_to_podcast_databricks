# Databricks notebook source
# MAGIC %run "/Workspace/nvidia blueprints/workflow/shared/monologue prompts"

# COMMAND ----------

# MAGIC %run "/Workspace/nvidia blueprints/workflow/shared/utils"

# COMMAND ----------

markdowns_table = "llm_agents_catalog.pdf_to_podcast.markdowns"
summary_output_table = "llm_agents_catalog.pdf_to_podcast.monologue_summary"
endpoint_host = ""
endpoint_name = "databricks-meta-llama-3-1-405b-instruct"

# COMMAND ----------

markdowns = spark.table(markdowns_table).collect()

# COMMAND ----------

template = FinancialSummaryPrompts.get_template("monologue_summary_prompt")
monologue_summarize_pdfs = []

for mkd in markdowns:
  prompt = template.render(text=mkd.markdown)
  response = score_model(prompt, endpoint_host, endpoint_name, True).json()["choices"][0]["message"]["content"]
  monologue_summarize_pdfs.append({"filename": mkd.filename, "monologue_summarize_pdf": response})

# COMMAND ----------

# Convert the list of dictionaries to a Spark DataFrame
df = spark.createDataFrame(monologue_summarize_pdfs)

# Write output to a delta table
df.write.format("delta").mode("overwrite").saveAsTable(summary_output_table)
