# Databricks notebook source
import requests

# COMMAND ----------

def score_model(question, host, endpoint, break_if_error = False):

    data = {
        "messages": 
            [ 
             {
                 "role": "user", 
                 "content": question
             }
            ]
           }

    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    headers = {"Context-Type": "text/json", "Authorization": f"Bearer {token}"}

    response = requests.post(
        url=f"{host}/serving-endpoints/{endpoint}/invocations", json=data, headers=headers
    )

    if response.status_code != 200:
        if break_if_error:
            raise Exception(f"Model Serving Endpoint {endpoint}: Expected status code 200 but got {response.status_code}")
        else:
            return ""
    else:
        return response

# COMMAND ----------

def unescape_unicode_string(s: str) -> str:
    """
    Convert escaped Unicode sequences to actual Unicode characters.

    Args:
        s (str): String potentially containing escaped Unicode sequences

    Returns:
        str: String with Unicode sequences converted to actual characters

    Example:
        >>> unescape_unicode_string("Hello\\u2019s World")
        "Hello's World"
    """
    # This handles both raw strings (with extra backslashes) and regular strings
    return s.encode("utf-8").decode("unicode-escape")
