"""Wire protocol helpers for CSE 434 DHT project (Group 33)."""
import json
GROUP_NUMBER = 33
PORT_MIN, PORT_MAX = 17500, 17999
MAX_DATAGRAM = 65507

def encode(obj):
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def decode(data):
    obj = json.loads(data.decode("utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("protocol message must be a JSON object")
    return obj

def ok_response(message="", **extra):
    return {"type":"manager-response", "code":"SUCCESS", "message":message, **extra}

def fail_response(message="", **extra):
    return {"type":"manager-response", "code":"FAILURE", "message":message, **extra}

def valid_port(port):
    return PORT_MIN <= int(port) <= PORT_MAX
