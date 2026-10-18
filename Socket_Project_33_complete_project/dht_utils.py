"""Dataset parsing and DHT hashing helpers."""
import csv, math
from pathlib import Path

def is_prime(n):
    if n < 2: return False
    if n % 2 == 0: return n == 2
    d = 3
    while d*d <= n:
        if n % d == 0: return False
        d += 2
    return True

def first_prime_greater_than(value):
    n = value + 1
    while not is_prime(n): n += 1
    return n

def load_year_csv(year, data_dir=None):
    base = Path(data_dir) if data_dir else Path(__file__).resolve().parent / "data"
    path = base / f"details-{int(year)}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}. Copy the official details-{year}.csv into the data/ folder.")
    with path.open("r", newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"Dataset {path} has no CSV header")
        fields = {name.strip().upper(): name for name in reader.fieldnames if name}
        event_key = fields.get("EVENT_ID") or fields.get("EVENT ID")
        if not event_key:
            raise ValueError(f"Dataset {path} has no EVENT_ID column")
        records=[]
        for line_no, row in enumerate(reader, start=2):
            if not row or not any((v or "").strip() for v in row.values()): continue
            raw = (row.get(event_key) or "").strip()
            try: event_id = int(raw)
            except ValueError: raise ValueError(f"Non-integer EVENT_ID {raw!r} at CSV line {line_no}")
            records.append({"event_id":event_id, "fields":row})
    return records, list(reader.fieldnames), str(path)

def hash_record(event_id, table_size, ring_size):
    pos = int(event_id) % int(table_size)
    return pos, pos % int(ring_size)

def prime_table_size(records):
    return first_prime_greater_than(2 * len(records))

def bucket_add(table, pos, record):
    table.setdefault(int(pos), []).append(record)

def bucket_find(table, pos, event_id):
    for item in table.get(int(pos), []):
        if int(item.get("event_id", -1)) == int(event_id): return item
    return None

def count_table(table):
    return sum(len(v) for v in table.values())
