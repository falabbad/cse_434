#!/usr/bin/env python3
import ipaddress
import socket
import sys
from typing import Dict, Optional, Tuple

from protocol import FAILURE, SUCCESS, PORT_MIN, PORT_MAX, decode, encode, make_response, valid_group_port


class Manager:
    def __init__(self, port: int):
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("0.0.0.0", port))

        self.peers: Dict[str, dict] = {}
        self.dht_exists = False
        self.dht_complete = False
        self.leader_name: Optional[str] = None
        self.dht_members = []

    @staticmethod
    def valid_name(name: str) -> bool:
        return 1 <= len(name) <= 15 and name.isalpha()

    @staticmethod
    def valid_ipv4(address: str) -> bool:
        try:
            return isinstance(ipaddress.ip_address(address), ipaddress.IPv4Address)
        except ValueError:
            return False

    def all_reserved_ports(self):
        ports = {self.port}
        for peer in self.peers.values():
            ports.add(peer["m_port"])
            ports.add(peer["p_port"])
        return ports

    def register(self, message: dict, sender: Tuple[str, int]) -> dict:
        if self.dht_exists:
            return make_response(FAILURE, "DHT already exists; registration is unavailable.")

        name = str(message.get("peer_name", ""))
        address = str(message.get("ip", ""))
        try:
            m_port = int(message.get("m_port"))
            p_port = int(message.get("p_port"))
        except (TypeError, ValueError):
            return make_response(FAILURE, "m-port and p-port must be integers.")

        if not self.valid_name(name):
            return make_response(FAILURE, "Peer name must be alphabetic and at most 15 characters.")
        if name in self.peers:
            return make_response(FAILURE, "Peer name is already registered.")
        if not self.valid_ipv4(address):
            return make_response(FAILURE, "Invalid IPv4 address.")
        if not valid_group_port(m_port) or not valid_group_port(p_port):
            return make_response(FAILURE, f"Ports must be in Group 33 range {PORT_MIN}-{PORT_MAX}.")
        if m_port == p_port:
            return make_response(FAILURE, "m-port and p-port must be different.")
        if m_port in self.all_reserved_ports() or p_port in self.all_reserved_ports():
            return make_response(FAILURE, "One or more supplied ports are already in use by the project.")

        self.peers[name] = {
            "ip": address,
            "m_port": m_port,
            "p_port": p_port,
            "state": "Free",
        }

        print(f"[MANAGER] REGISTERED {name} IP={address} M={m_port} P={p_port} STATE=Free", flush=True)
        return make_response(SUCCESS, f"{name} registered successfully.")

    def setup_dht(self, message: dict) -> dict:
        if self.dht_exists:
            return make_response(FAILURE, "A DHT already exists.")

        leader = str(message.get("peer_name", ""))
        try:
            n = int(message.get("n"))
            year = int(message.get("year"))
        except (TypeError, ValueError):
            return make_response(FAILURE, "n and year must be integers.")

        if leader not in self.peers:
            return make_response(FAILURE, "Leader peer is not registered.")
        if self.peers[leader]["state"] != "Free":
            return make_response(FAILURE, "Leader peer is not Free.")
        if n < 3:
            return make_response(FAILURE, "DHT size n must be at least 3.")
        if len(self.peers) < n:
            return make_response(FAILURE, "Fewer than n users are registered.")
        if not (1950 <= year <= 2019):
            return make_response(FAILURE, "Year must be between 1950 and 2019.")

        import random
        free_others = [name for name, peer in self.peers.items() if name != leader and peer["state"] == "Free"]
        random.shuffle(free_others)
        selected = [leader] + free_others[: n - 1]

        self.dht_exists = True
        self.dht_complete = False
        self.leader_name = leader
        self.dht_members = selected

        self.peers[leader]["state"] = "Leader"
        for name in selected[1:]:
            self.peers[name]["state"] = "InDHT"

        tuples = [
            {
                "peer_name": name,
                "ip": self.peers[name]["ip"],
                "p_port": self.peers[name]["p_port"],
            }
            for name in selected
        ]

        print(f"[MANAGER] SETUP-DHT leader={leader} n={n} year={year}", flush=True)
        print(f"[MANAGER] DHT MEMBERS (leader first): {selected}", flush=True)
        return make_response(
            SUCCESS,
            "DHT setup accepted.",
            year=year,
            ring_size=n,
            peers=tuples,
        )

    def dht_complete_command(self, message: dict) -> dict:
        peer_name = str(message.get("peer_name", ""))
        if not self.dht_exists:
            return make_response(FAILURE, "No DHT exists.")
        if peer_name != self.leader_name:
            return make_response(FAILURE, "Only the DHT leader may send dht-complete.")
        if self.peers.get(peer_name, {}).get("state") != "Leader":
            return make_response(FAILURE, "Sender is not the current DHT leader.")

        self.dht_complete = True
        print(f"[MANAGER] DHT-COMPLETE received from leader {peer_name}", flush=True)
        return make_response(SUCCESS, "DHT creation completed successfully.")

    def dispatch(self, message: dict, sender: Tuple[str, int]) -> dict:
        command = str(message.get("command", "")).lower()
        peer_name = message.get("peer_name", "")
        print(f"[MANAGER] RX command={command} peer={peer_name} from={sender[0]}:{sender[1]}", flush=True)

        if self.dht_exists and not self.dht_complete and command != "dht-complete":
            return make_response(FAILURE, "DHT is being constructed; only dht-complete is accepted.")

        if command == "register":
            return self.register(message, sender)
        if command == "setup-dht":
            return self.setup_dht(message)
        if command == "dht-complete":
            return self.dht_complete_command(message)

        return make_response(FAILURE, "Command is not part of Milestone 1.")

    def response_target(self, message: dict, sender: Tuple[str, int]) -> Tuple[str, int]:
        name = str(message.get("peer_name", ""))
        if name in self.peers:
            return self.peers[name]["ip"], self.peers[name]["m_port"]
        return sender

    def run(self):
        print(f"[MANAGER] Group 33 manager listening on UDP 0.0.0.0:{self.port}", flush=True)
        print(f"[MANAGER] Allowed project port range: {PORT_MIN}-{PORT_MAX}", flush=True)
        while True:
            try:
                data, sender = self.socket.recvfrom(65535)
                message = {}
                try:
                    message = decode(data)
                    response = self.dispatch(message, sender)
                except Exception as exc:
                    response = make_response(FAILURE, f"Malformed request: {exc}")
                target = self.response_target(message if 'message' in locals() and isinstance(message, dict) else {}, sender)
                self.socket.sendto(encode(response), target)
                print(f"[MANAGER] TX {response['code']} -> {target[0]}:{target[1]} : {response['message']}", flush=True)
            except KeyboardInterrupt:
                print("\n[MANAGER] Stopped.")
                break
            except Exception as exc:
                print(f"[MANAGER] ERROR: {exc}", flush=True)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 manager.py <manager-port>")
        sys.exit(1)

    try:
        port = int(sys.argv[1])
    except ValueError:
        print("ERROR: manager port must be an integer.")
        sys.exit(1)

    if not valid_group_port(port):
        print(f"ERROR: Group 33 manager port must be {PORT_MIN}-{PORT_MAX}.")
        sys.exit(1)

    manager = Manager(port)
    manager.run()


if __name__ == "__main__":
    main()
