# Databricks notebook source
# MAGIC %md
# MAGIC # Dependencies

# COMMAND ----------

# MAGIC %pip install docling elevenlabs

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import httpx
from elevenlabs.client import ElevenLabs
from pydantic import BaseModel, Field
from typing import Optional, Union, Literal
import time
import os

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters

# COMMAND ----------

eleven_labs_api_key = ""
voice_1 = "iP95p4xoKVk53GoZ742B"
monologue_table = "llm_agents_catalog.pdf_to_podcast.monologue"
output_path = "/Volumes/llm_agents_catalog/pdf_to_podcast/podecast_output"
output_file_name = "realty_income.mp3"

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Functions

# COMMAND ----------

class VoiceInfo(BaseModel):
    voice_id: str
    name: str
    description: Optional[str] = None

# COMMAND ----------

httpx_client = httpx.Client()
eleven_labs_client = ElevenLabs(
            api_key=eleven_labs_api_key,
            httpx_client=httpx_client,
            timeout=120,
        )

# COMMAND ----------

def _convert_text(text: str, voice_id: str) -> bytes:
  """Convert text to speech using ElevenLabs"""
  audio_stream = eleven_labs_client.text_to_speech.convert(
      text=text,
      voice_id=voice_id,
      model_id="eleven_monolingual_v1",
      output_format="mp3_44100_128",
      voice_settings={"stability": 0.5, "similarity_boost": 0.75, "style": 0.0},
  )
  return b"".join(chunk for chunk in audio_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create TTS

# COMMAND ----------

dialogue = spark.table(monologue_table).collect()[0][1]

# COMMAND ----------

DEFAULT_VOICE_MAPPING = {"speaker-1": voice_1}
for d in dialogue:
  speaker = d["speaker"]
  d["voice_id"] = DEFAULT_VOICE_MAPPING[speaker]

# COMMAND ----------

print(dialogue)

# COMMAND ----------

combined_audio = []

for task in dialogue:

  text = task["text"]
  voice_id = task["voice_id"]

  combined_audio += _convert_text(text, voice_id)
  time.sleep(2)

print(combined_audio)

# COMMAND ----------

combined_audio_bytes = b''.join([bytes([b]) for b in combined_audio])

# Save the audio file locally
with open(os.path.join(output_path, output_file_name), "wb") as f:
    f.write(combined_audio_bytes)

print(f"Audio file written to {output_path}")
