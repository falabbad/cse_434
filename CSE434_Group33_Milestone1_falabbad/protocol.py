import json

SUCCESS = "SUCCESS"
FAILURE = "FAILURE"

GROUP_NUMBER = 33
PORT_MIN = 17500
PORT_MAX = 17999


def encode(message: dict) -> bytes:
    return json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def decode(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))


def make_response(code: str, message: str = "", **extra) -> dict:
    result = {"type": "manager-response", "code": code, "message": message}
    result.update(extra)
    return result


def valid_group_port(port: int) -> bool:
    return PORT_MIN <= port <= PORT_MAX
