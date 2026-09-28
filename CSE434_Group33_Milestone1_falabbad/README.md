# CSE 434 Fall 2026 — Socket Programming Project — Milestone 1
## Group 33 / Peer name: falabbad

This implementation is for Milestone 1 only. It implements the manager commands `register`, `setup-dht`, and `dht-complete`, plus the peer-to-peer messages required to construct and populate the DHT.

## Group 33 port range

Group 33 uses UDP ports **17500–17999**. The included example uses:

| Process | M-port | P-port |
|---|---:|---:|
| Manager | 17500 | — |
| falabbad (leader) | 17501 | 17502 |
| peerone | 17503 | 17504 |
| peertwo | 17505 | 17506 |

All six ports are unique and inside the Group 33 range.

## Files

- `manager.py` — manager UDP server.
- `peer.py` — peer process and DHT implementation.
- `protocol.py` — JSON message encoding and project constants.
- `dht_utils.py` — CSV loading, prime-size calculation, hashing, and collision-safe local storage.
- `data/details-1950.csv` — supplied 1950 storm dataset.

## Message format

Messages are UTF-8 JSON objects. This makes each field explicit and easy to trace in the demo.

Manager request examples:

```json
{"command":"register","peer_name":"falabbad","ip":"10.0.0.1","m_port":17501,"p_port":17502}
```

```json
{"command":"setup-dht","peer_name":"falabbad","n":3,"year":1950}
```

```json
{"command":"dht-complete","peer_name":"falabbad"}
```

Peer-to-peer messages include `set-id`, `store`, `store-complete`, `count-report`, and `set-id-ack`.

## Dataset/hash implementation

The supplied 1950 CSV contains 223 data records plus the header. Therefore:

- `ell = 223`
- `2 * ell = 446`
- first prime greater than 446 = **449**
- `position = EVENT_ID mod 449`
- `destination_id = position mod n`

For `n=3`, the destination IDs are 0, 1, and 2.

The complete CSV row is retained. The implementation does not assume the dataset has only 14 columns.

## Important ring behavior

The leader sends a non-local `store` only to its right neighbor. A receiving peer either stores the record if its ID matches the destination ID or forwards the message to its own right neighbor. Thus records follow the logical ring and are never sent directly by the leader to a non-neighbor destination.

A `store-complete` marker is sent after the leader's STORE messages. Because each node forwards the marker after its preceding STORE messages, it provides a deterministic barrier before final counting.

## Compile/check

From this directory:

```bash
python3 -m py_compile manager.py peer.py protocol.py dht_utils.py
```

## Run

### Terminal 1 — manager

```bash
python3 manager.py 17500
```

### Terminal 2 — falabbad

```bash
python3 peer.py <MANAGER_IP> 17500
```

Then:

```text
register falabbad <FALABBAD_HOST_IP> 17501 17502
```

### Terminal 3 — peerone

```bash
python3 peer.py <MANAGER_IP> 17500
```

Then:

```text
register peerone <PEERONE_HOST_IP> 17503 17504
```

### Terminal 4 — peertwo

```bash
python3 peer.py <MANAGER_IP> 17500
```

Then:

```text
register peertwo <PEERTWO_HOST_IP> 17505 17506
```

### Start the DHT from falabbad

On the `falabbad` terminal:

```text
setup-dht falabbad 3 1950
```

The manager selects the three registered peers, with `falabbad` fixed as the leader. The leader assigns IDs 0, 1, and 2 according to the tuple order returned by the manager.

The leader then reads `data/details-1950.csv`, hashes all 223 records, sends non-local records around the ring, collects the final counts, and sends `dht-complete` to the manager.

## Finding IP addresses

On each host, use a reachable IPv4 address, for example:

```bash
hostname -I
```

Do not use `127.0.0.1` when the process must communicate with a different machine.

## Demo checklist

1. Compile/check all source files.
2. Start manager.
3. Start three peer processes.
4. Register all three peers.
5. Run `setup-dht falabbad 3 1950` from the leader.
6. Show SET-ID messages and acknowledgements.
7. Show ring STORE forwarding.
8. Show final record count at each node.
9. Show `DHT-COMPLETE` received by the manager.

The expected total for the supplied 1950 file is **223 records**.
