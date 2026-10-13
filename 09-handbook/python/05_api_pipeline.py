# ============================================================
# Pattern 05: API -> JSON -> pandas DataFrame
# (validation, logging, HTTP error handling, retry config)
#
# Source: https://jsonplaceholder.typicode.com/users (public test API)
#
# Problem:
#   Fetch records from an HTTP API, keep only the fields we need,
#   and check data quality before using the data.
#
# Pattern:
#   requests GET (with timeout) -> raise_for_status() -> .json()
#   -> select fields into dicts -> DataFrame -> assert checks -> log.
#
# Verified (normal run):
#   status 200, 10 users, columns [user_id, name, email],
#   all validation asserts pass, rows in == rows out (10 = 10).
#
# NOT verified yet:
#   Retry behaviour on error paths (404, 503, timeout).
#   The retry policy is configured, but it was never triggered.
#
# Notes:
#   * Retry only applies to GET and to the status codes in
#     status_forcelist. 4xx errors (400/401/403/404) are not retried.
#   * `raise` after logging the error: do not hide failures
#     from the caller (for example Airflow).
#   * assert is fine for learning; production code usually raises
#     explicit errors (python -O removes assert lines).
# ============================================================

import logging

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

url = "https://jsonplaceholder.typicode.com/users"

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

session = requests.Session()
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

try:
    response = session.get(url, timeout=10)
    response.raise_for_status()

    logging.info("API request successful: %s", response.status_code)

    users = response.json()

except requests.exceptions.RequestException as error:
    logging.error("API request failed: %s", error)
    raise

cleaned_users = []

for user in users:
    cleaned_user = {"user_id": user["id"], "name": user["name"], "email": user["email"]}
    cleaned_users.append(cleaned_user)

df = pd.DataFrame(cleaned_users)

assert len(df) > 0, "DataFrame is empty!"
assert list(df.columns) == ["user_id", "name", "email"], "Unexpected columns!"
assert df["user_id"].notna().all(), "Missing user ID!"
assert df["user_id"].is_unique, "Duplicate user IDs!"
assert df["email"].notna().all(), "Missing email!"
assert len(df) == len(users), "Rows lost while building DataFrame!"

logging.info("Validation passed")
logging.info("API pipeline completed successfully")
logging.info("Total users processed: %s", len(df))
