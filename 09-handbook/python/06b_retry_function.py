# Pattern 06b: Retry without global state
# fetch_with_retry(max_attempts): try, return on success, raise RuntimeError after the last failed attempt.
# Verified: fails twice, succeeds on attempt 3. Not verified: waiting between attempts (no sleep/backoff).


def fetch_data(attempt_number):
    if attempt_number < 3:
        raise ConnectionError("Server temporarily unavailable")

    return {"status": "success", "records": 10}


def fetch_with_retry(max_attempts):
    for attempt in range(1, max_attempts + 1):
        try:
            result = fetch_data(attempt)
            return result

        except ConnectionError as error:
            print("Attempt", attempt, "failed:", error)

    raise RuntimeError("All retry attempts failed!")


data = fetch_with_retry(3)

print("Final data:", data)
