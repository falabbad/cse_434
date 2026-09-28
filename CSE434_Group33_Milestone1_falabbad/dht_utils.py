import csv
import os
from typing import Dict, List, Tuple

# The ASU storm-data file supplied for this project contains 51 columns.
# We intentionally preserve the complete row rather than assuming 14 columns.
REQUIRED_HEADERS = {"EVENT_ID", "YEAR"}


def is_prime(number: int) -> bool:
    if number < 2:
        return False
    if number == 2:
        return True
    if number % 2 == 0:
        return False
    divisor = 3
    while divisor * divisor <= number:
        if number % divisor == 0:
            return False
        divisor += 2
    return True


def first_prime_greater_than(number: int) -> int:
    candidate = number + 1
    while not is_prime(candidate):
        candidate += 1
    return candidate


def load_year_csv(path: str) -> Tuple[List[dict], List[str]]:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Dataset not found: {path}")

    records: List[dict] = []
    with open(path, "r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")

        missing = REQUIRED_HEADERS - set(reader.fieldnames)
        if missing:
            raise ValueError(f"CSV is missing required columns: {sorted(missing)}")

        headers = list(reader.fieldnames)
        for line_number, row in enumerate(reader, start=2):
            if row.get("EVENT_ID", "").strip() == "":
                raise ValueError(f"Missing EVENT_ID at CSV line {line_number}")
            try:
                int(row["EVENT_ID"])
            except ValueError as exc:
                raise ValueError(
                    f"EVENT_ID must be numeric at CSV line {line_number}: {row['EVENT_ID']}"
                ) from exc
            records.append(row)

    return records, headers


def calculate_hash(event_id: str, table_size: int, ring_size: int) -> Tuple[int, int]:
    event_number = int(event_id)
    position = event_number % table_size
    destination_id = position % ring_size
    return position, destination_id


def store_record(hash_table: Dict[int, List[dict]], position: int, record: dict) -> None:
    # Separate chaining prevents a collision from overwriting an earlier record.
    hash_table.setdefault(position, []).append(record)


def count_records(hash_table: Dict[int, List[dict]]) -> int:
    return sum(len(bucket) for bucket in hash_table.values())
