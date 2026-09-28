#!/usr/bin/env python3
import os
import socket
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

from protocol import FAILURE, SUCCESS, decode, encode
from dht_utils import calculate_hash, count_records, first_prime_greater_than, load_year_csv, store_record


class Peer:
    def __init__(self, manager_ip: str, manager_port: int):
        self.manager_ip = manager_ip
        self.manager_port = manager_port

        self.peer_name: Optional[str] = None
        self.host_ip: Optional[str] = None
        self.m_port: Optional[int] = None
        self.p_port: Optional[int] = None

        self.manager_socket: Optional[socket.socket] = None
        self.peer_socket: Optional[socket.socket] = None
        self.listener_thread: Optional[threading.Thread] = None
        self.running = True
        self.state = "Free"

        self.node_id: Optional[int] = None
        self.ring_size: Optional[int] = None
        self.members: List[dict] = []
        self.right_neighbor: Optional[Tuple[str, int]] = None

        self.hash_table: Dict[int, List[dict]] = {}
        self.record_count = 0
        self.store_complete_event = threading.Event()
        self.count_complete_event = threading.Event()
        self.set_id_ack_event = threading.Event()
        self.acks = set()
        self.final_counts: Dict[int, int] = {}
        self.count_report_received: Optional[dict] = None
        self.setup_year: Optional[int] = None

    def log(self, text: str):
        print(f"[PEER {self.peer_name or 'UNREGISTERED'}] {text}", flush=True)

    def send_peer(self, message: dict, target: Tuple[str, int]):
        assert self.peer_socket is not None
        self.peer_socket.sendto(encode(message), target)
        self.log(f"TX P2P {message.get('type')} -> {target[0]}:{target[1]}")

    def manager_request(self, message: dict, timeout: float = 10.0) -> dict:
        assert self.manager_socket is not None
        self.manager_socket.settimeout(timeout)
        self.manager_socket.sendto(encode(message), (self.manager_ip, self.manager_port))
        self.log(f"TX MANAGER {message.get('command')} -> {self.manager_ip}:{self.manager_port}")
        data, sender = self.manager_socket.recvfrom(65535)
        response = decode(data)
        self.log(f"RX MANAGER {response.get('code')} from {sender[0]}:{sender[1]} : {response.get('message')}")
        return response

    def start_listener(self):
        self.listener_thread = threading.Thread(target=self.p2p_listener, daemon=True)
        self.listener_thread.start()

    def p2p_listener(self):
        assert self.peer_socket is not None
        self.peer_socket.settimeout(1.0)
        while self.running:
            try:
                data, sender = self.peer_socket.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break

            try:
                message = decode(data)
                msg_type = message.get("type")
                self.log(f"RX P2P {msg_type} from {sender[0]}:{sender[1]}")
                if msg_type == "set-id":
                    self.handle_set_id(message, sender)
                elif msg_type == "store":
                    self.handle_store(message)
                elif msg_type == "store-complete":
                    self.handle_store_complete(message)
                elif msg_type == "count-report":
                    self.handle_count_report(message)
                elif msg_type == "set-id-ack":
                    self.handle_set_id_ack(message)
                else:
                    self.log(f"Ignoring unsupported P2P message: {msg_type}")
            except Exception as exc:
                self.log(f"P2P ERROR: {exc}")

    def handle_set_id(self, message: dict, sender):
        self.node_id = int(message["node_id"])
        self.ring_size = int(message["ring_size"])
        self.members = message["members"]
        self.state = "Leader" if self.node_id == 0 else "InDHT"

        right_id = (self.node_id + 1) % self.ring_size
        right = self.members[right_id]
        self.right_neighbor = (right["ip"], int(right["p_port"]))

        self.log(
            f"SET-ID node_id={self.node_id}, ring_size={self.ring_size}, "
            f"right-neighbor={right['peer_name']}@{right['ip']}:{right['p_port']}"
        )

        ack = {
            "type": "set-id-ack",
            "peer_name": self.peer_name,
            "node_id": self.node_id,
        }
        self.send_peer(ack, sender)

    def handle_set_id_ack(self, message: dict):
        node_id = int(message["node_id"])
        self.acks.add(node_id)
        self.log(f"SET-ID ACK received for node {node_id} ({len(self.acks)}/{self.ring_size - 1 if self.ring_size else '?'})")
        if self.ring_size and len(self.acks) >= self.ring_size - 1:
            self.set_id_ack_event.set()

    def handle_store(self, message: dict):
        destination_id = int(message["destination_id"])
        position = int(message["position"])
        record = message["record"]

        if destination_id == self.node_id:
            store_record(self.hash_table, position, record)
            self.record_count += 1
            self.log(f"STORE consumed locally: position={position}, total_local={self.record_count}")
        else:
            if self.right_neighbor is None:
                raise RuntimeError("Right neighbor is not initialized")
            self.log(f"STORE forwarding: destination_id={destination_id}, position={position}")
            self.send_peer(message, self.right_neighbor)

    def handle_store_complete(self, message: dict):
        origin = message.get("origin")
        if self.peer_name == origin:
            self.log("STORE-COMPLETE returned to origin; all preceding ring STORE messages have been processed.")
            self.store_complete_event.set()
            return

        if self.right_neighbor is None:
            raise RuntimeError("Right neighbor is not initialized")
        self.send_peer(message, self.right_neighbor)

    def handle_count_report(self, message: dict):
        origin = message.get("origin")
        counts = {int(k): int(v) for k, v in message.get("counts", {}).items()}

        if self.peer_name == origin:
            counts[int(self.node_id)] = self.record_count
            self.count_report_received = counts
            self.final_counts = counts
            self.log(f"COUNT-REPORT returned: {dict(sorted(counts.items()))}")
            self.count_complete_event.set()
            return

        counts[int(self.node_id)] = self.record_count
        message["counts"] = {str(k): v for k, v in counts.items()}
        if self.right_neighbor is None:
            raise RuntimeError("Right neighbor is not initialized")
        self.send_peer(message, self.right_neighbor)

    def register_command(self, command: str):
        parts = command.split()
        if len(parts) != 5:
            print("Usage: register <peer-name> <IPv4-address> <m-port> <p-port>")
            return

        _, name, ip, m_port_text, p_port_text = parts
        try:
            m_port = int(m_port_text)
            p_port = int(p_port_text)
        except ValueError:
            print("ERROR: ports must be integers")
            return

        if self.manager_socket is not None:
            print("ERROR: this peer is already registered/initialized.")
            return

        try:
            self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.manager_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.manager_socket.bind(("0.0.0.0", m_port))

            self.peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Enlarge the receive buffer so a burst of back-to-back STORE
            # datagrams during DHT population does not overflow the kernel
            # socket buffer and get silently dropped by the OS.
            try:
                self.peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
            except OSError:
                pass
            self.peer_socket.bind(("0.0.0.0", p_port))
        except OSError as exc:
            print(f"ERROR: could not bind requested ports: {exc}")
            if self.manager_socket:
                self.manager_socket.close()
            if self.peer_socket:
                self.peer_socket.close()
            self.manager_socket = None
            self.peer_socket = None
            return

        self.peer_name = name
        self.host_ip = ip
        self.m_port = m_port
        self.p_port = p_port
        self.start_listener()

        response = self.manager_request({
            "command": "register",
            "peer_name": name,
            "ip": ip,
            "m_port": m_port,
            "p_port": p_port,
        })

        if response.get("code") != SUCCESS:
            print("Registration failed. You may restart this peer with different values.")
            return

        self.state = "Free"
        print(f"REGISTER SUCCESS: {name} is now Free.")

    def setup_dht_command(self, command: str):
        parts = command.split()
        if len(parts) != 4:
            print("Usage: setup-dht <peer-name> <n> <YYYY>")
            return
        _, leader_name, n_text, year_text = parts
        try:
            n = int(n_text)
            year = int(year_text)
        except ValueError:
            print("ERROR: n and year must be integers")
            return

        if self.peer_name is None or self.manager_socket is None:
            print("ERROR: register this peer first.")
            return

        response = self.manager_request({
            "command": "setup-dht",
            "peer_name": leader_name,
            "n": n,
            "year": year,
        }, timeout=15.0)

        if response.get("code") != SUCCESS:
            print(f"SETUP-DHT FAILED: {response.get('message')}")
            return

        members = response["peers"]
        if not members or members[0]["peer_name"] != self.peer_name:
            print("SETUP-DHT response was not addressed to this leader process.")
            return

        self.setup_year = int(response["year"])
        self.ring_size = int(response["ring_size"])
        self.members = members
        self.node_id = 0
        self.state = "Leader"
        right = members[1 % self.ring_size]
        self.right_neighbor = (right["ip"], int(right["p_port"]))
        self.hash_table.clear()
        self.record_count = 0
        self.acks.clear()
        self.set_id_ack_event.clear()
        self.store_complete_event.clear()
        self.count_complete_event.clear()

        self.log(f"SETUP-DHT SUCCESS. Leader node_id=0, ring_size={self.ring_size}, year={self.setup_year}")
        self.log("Leader tuple is first in the manager's returned list.")

        # The specification requires the leader to assign IDs 0..n-1 and send
        # each peer its ID, ring size, and the complete peer tuple list.
        for node_id, member in enumerate(members):
            if node_id == 0:
                continue
            target = (member["ip"], int(member["p_port"]))
            message = {
                "type": "set-id",
                "node_id": node_id,
                "ring_size": self.ring_size,
                "members": members,
            }
            self.send_peer(message, target)

        expected_acks = self.ring_size - 1
        if not self.set_id_ack_event.wait(timeout=30):
            print(f"ERROR: timed out waiting for SET-ID ACKs ({len(self.acks)}/{expected_acks}).")
            return

        print("All SET-ID acknowledgements received. Starting dataset hashing and ring STORE forwarding.")
        self.populate_dht()

    def populate_dht(self):
        if self.setup_year is None or self.ring_size is None:
            raise RuntimeError("DHT has not been initialized")

        dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", f"details-{self.setup_year}.csv")
        records, _headers = load_year_csv(dataset_path)
        ell = len(records)
        table_size = first_prime_greater_than(2 * ell)

        print(f"[PEER {self.peer_name}] DATASET: {dataset_path}")
        print(f"[PEER {self.peer_name}] RECORDS (ell): {ell}")
        print(f"[PEER {self.peer_name}] LOCAL HASH TABLE SIZE (first prime > 2*ell): {table_size}")
        print(f"[PEER {self.peer_name}] HASH: position = EVENT_ID mod {table_size}; destination_id = position mod {self.ring_size}")

        for record_number, record in enumerate(records, start=1):
            position, destination_id = calculate_hash(record["EVENT_ID"], table_size, self.ring_size)
            if destination_id == self.node_id:
                store_record(self.hash_table, position, record)
                self.record_count += 1
            else:
                message = {
                    "type": "store",
                    "origin": self.peer_name,
                    "position": position,
                    "destination_id": destination_id,
                    "record": record,
                }
                # Critical project requirement: the leader never sends STORE
                # directly to the destination. It always sends to its right
                # neighbor; peers then forward around the logical ring.
                self.send_peer(message, self.right_neighbor)
                # Tiny pacing delay: UDP has no flow control, so firing
                # hundreds of datagrams back-to-back can overflow a
                # receiver's socket buffer and silently drop packets.
                time.sleep(0.002)

            if record_number % 25 == 0 or record_number == ell:
                print(f"[PEER {self.peer_name}] HASHED {record_number}/{ell} records", flush=True)

        # A marker sent after all STORE datagrams provides a deterministic
        # barrier without an arbitrary sleep. Each node forwards it only after
        # processing all preceding STORE datagrams from its left neighbor.
        self.send_peer({"type": "store-complete", "origin": self.peer_name}, self.right_neighbor)

        if not self.store_complete_event.wait(timeout=60):
            print("ERROR: timed out waiting for STORE-COMPLETE to complete the ring.")
            return

        self.collect_final_counts()

    def collect_final_counts(self):
        self.count_complete_event.clear()
        self.final_counts = {}
        message = {
            "type": "count-report",
            "origin": self.peer_name,
            "counts": {},
        }
        self.send_peer(message, self.right_neighbor)

        if not self.count_complete_event.wait(timeout=30):
            print("ERROR: timed out waiting for COUNT-REPORT.")
            return

        print("\n========== FINAL DHT RECORD COUNTS ==========")
        total = 0
        for node_id in range(self.ring_size):
            member = self.members[node_id]
            count = int(self.final_counts.get(node_id, 0))
            total += count
            print(f"Node {node_id}: {member['peer_name']} -> {count} records")
        print(f"TOTAL STORED RECORDS: {total}")
        print("============================================")

        if total != self.record_count + sum(
            count for node_id, count in self.final_counts.items() if int(node_id) != 0
        ):
            # This condition is intentionally not used for the final correctness
            # check; node 0's record_count is already included in final_counts.
            pass

        # The dataset record count is available from the table counts after the
        # ring has completed. Re-load only the count, not the records.
        dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", f"details-{self.setup_year}.csv")
        records, _ = load_year_csv(dataset_path)
        if total != len(records):
            print(f"ERROR: total {total} does not equal dataset record count {len(records)}. DHT-COMPLETE will NOT be sent.")
            return

        self.send_dht_complete()

    def send_dht_complete(self):
        response = self.manager_request({
            "command": "dht-complete",
            "peer_name": self.peer_name,
        })
        if response.get("code") == SUCCESS:
            print("DHT-COMPLETE SUCCESS: manager confirms DHT creation is complete.")
        else:
            print(f"DHT-COMPLETE FAILED: {response.get('message')}")

    def manual_dht_complete(self):
        if self.peer_name is None:
            print("ERROR: register first.")
            return
        response = self.manager_request({
            "command": "dht-complete",
            "peer_name": self.peer_name,
        })
        print(f"DHT-COMPLETE: {response.get('code')} - {response.get('message')}")

    def close(self):
        self.running = False
        for sock in (self.manager_socket, self.peer_socket):
            if sock:
                try:
                    sock.close()
                except OSError:
                    pass

    def run(self):
        print("CSE 434 Fall 2026 - Milestone 1 Peer")
        print("Required CLI: python3 peer.py <manager-ip> <manager-port>")
        print("Type 'help' for commands.")
        while self.running:
            try:
                command = input(f"peer[{self.peer_name or 'unregistered'}]> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not command:
                continue

            lower = command.lower()
            if lower.startswith("register"):
                self.register_command(command)
            elif lower.startswith("setup-dht"):
                self.setup_dht_command(command)
            elif lower == "dht-complete":
                self.manual_dht_complete()
            elif lower == "help":
                print("register <peer-name> <IPv4-address> <m-port> <p-port>")
                print("setup-dht <peer-name> <n> <YYYY>")
                print("dht-complete")
                print("exit")
            elif lower == "exit" or lower == "quit":
                break
            else:
                print("Unknown command. Type 'help'.")
        self.close()


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 peer.py <manager-ip> <manager-port>")
        sys.exit(1)

    manager_ip = sys.argv[1]
    try:
        manager_port = int(sys.argv[2])
    except ValueError:
        print("ERROR: manager port must be an integer.")
        sys.exit(1)

    peer = Peer(manager_ip, manager_port)
    peer.run()


if __name__ == "__main__":
    main()
