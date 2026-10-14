# ============================================================
# Pattern 06: Retry basics (for / try-except / break / for-else)
#
# Problem:
#   An operation can fail temporarily. Try it a limited number
#   of times, stop on success, and fail loudly if all attempts fail.
#
# Pattern:
#   for attempt in range(1, 4)
#     try: call -> success -> break
#     except: log the failed attempt, try again
#   else: raise RuntimeError   (runs only if no break happened)
#
# Verified:
#   - success on attempt 3 (fails twice, then succeeds)
#   - with attempt_count < 5: three failures, then RuntimeError,
#     exit code 1
#
# Notes:
#   * `global` is used only to simulate a flaky server.
#     Cleaner version without global: next session.
#   * No waiting between attempts yet (no time.sleep / backoff).
# ============================================================

# attempts = [False, False, True]

# for attempt_number, success in enumerate(attempts, start=1):
#     print("Attempt:", attempt_number)

#     if success:
#         print("Request successful!")
#         break

#     print("Request failed!")

attempt_count = 0


def fetch_data():
    global attempt_count

    attempt_count += 1

    if attempt_count < 3:
        raise ConnectionError("Server temporarily unavailable")

    return {"status": "success", "records": 10}


for attempt in range(1, 4):
    try:
        result = fetch_data()
        print("Success:", result)
        break

    except ConnectionError as error:
        print("Attempt", attempt, "failed:", error)

else:
    raise RuntimeError("All retry attempts failed!")
