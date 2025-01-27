# Databricks notebook source
# MAGIC %pip install -U pydantic

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "/Workspace/nvidia blueprints/workflow/shared/monologue prompts"

# COMMAND ----------

# MAGIC %run "/Workspace/nvidia blueprints/workflow/shared/podcast types"

# COMMAND ----------

# MAGIC %run "/Workspace/nvidia blueprints/workflow/shared/utils"

# COMMAND ----------

import json
from pyspark.sql import Row

# COMMAND ----------

raw_outline_table = "llm_agents_catalog.pdf_to_podcast.monologue_raw_outline"
markdowns_table = "llm_agents_catalog.pdf_to_podcast.markdowns"
speaker_1_name="Alex"
guide="Focus on the company's earnings and the key points driving it's growth"
monolog_output_table = "llm_agents_catalog.pdf_to_podcast.monologue"

# COMMAND ----------

raw_outline_list = spark.table(raw_outline_table).collect()
raw_outline = raw_outline_list[0][1]
files = raw_outline_list[0][0]

# COMMAND ----------

print(raw_outline_list)

# COMMAND ----------

# Convert DataFrame to a list of dictionaries
markdown_results = [row.asDict() for row in spark.table(markdowns_table).collect()]

# COMMAND ----------

template = FinancialSummaryPrompts.get_template("monologue_transcript_prompt")
prompt = template.render(
        raw_outline=raw_outline,
        documents=markdown_results,
        focus=guide
        if guide
        else "key financial metrics and performance indicators",
        speaker_1_name=speaker_1_name,
    )

monologue = score_model(prompt, "https://adb-984752964297111.11.azuredatabricks.net", "databricks-meta-llama-3-1-405b-instruct", True).json()["choices"][0]["message"]["content"]

# COMMAND ----------

schema = Conversation.model_json_schema()

template = FinancialSummaryPrompts.get_template("monologue_dialogue_prompt")

prompt = template.render(
        speaker_1_name=speaker_1_name,
        text=monologue,
        schema=json.dumps(schema, indent=2),
    )

conversation = score_model(prompt, "https://adb-984752964297111.11.azuredatabricks.net", "databricks-meta-llama-3-1-405b-instruct", True).json()["choices"][0]["message"]["content"]

if conversation.startswith("```") and conversation.endswith("```"):
  conversation = conversation[3:-3]

print(conversation)

conversation_json = json.loads(conversation)

if "dialogue" in conversation_json:
  for entry in conversation_json["dialogue"]:
    if "text" in entry:
      entry["text"] = unescape_unicode_string(entry["text"])

dialogue = conversation_json["dialogue"]

# COMMAND ----------

data = [Row(files=files, monologue=dialogue)]
spark.createDataFrame(data).write.mode("overwrite").saveAsTable(monolog_output_table)
