# CSE 434 Socket Programming Project — Group 33

Peer name for the leader demo: `falabbad`. Language: Python 3. No third-party Python packages are needed for the application.

## Important scope

This is a full-project implementation scaffold for the manager commands `register`, `setup-dht`, `dht-complete`, `query-dht`, `leave-dht`, `join-dht`, `dht-rebuilt`, `deregister`, `teardown-dht`, and `teardown-complete`, with peer-to-peer protocols for DHT construction, record storage, hot-potato querying, teardown, leave, and join. The implementation was locally exercised for registration, setup of a three-node DHT using the supplied 1950 dataset, record counts, and `dht-complete`. Before claiming a full-project demonstration is complete, run the tests and demonstrate query/join/leave/teardown on the actual end-hosts.

## Group 33 ports

The assigned range is **17500–17999**. Example local allocation:

| Process | Manager port | Peer port |
|---|---:|---:|
| Manager | 17500 | — |
| falabbad | 17501 | 17502 |
| peerone | 17503 | 17504 |
| peertwo | 17505 | 17506 |
| peerthree | 17507 | 17508 |
| peerfour | 17509 | 17510 |
| peerfive | 17511 | 17512 |

Every process must use the real reachable IPv4 address of its host for registration. Do not use 127.0.0.1 for peers on different hosts.

## Files

- `manager.py`: manager UDP server and all ten manager commands.
- `peer.py`: interactive peer CLI, peer-to-peer listener and protocols.
- `protocol.py`: JSON datagram protocol and Group 33 port constants.
- `dht_utils.py`: CSV parser, prime size, hash mapping and local bucket helpers.
- `download_dataset.py`: fetches an official NOAA/NCEI yearly details file into `data/`.
- `data/details-1950.csv`: user-supplied dataset for Milestone 1.
- `docs/`: design report and diagrams.

## Dataset requirement for the full-project video

The assignment's full-project demo requires `details-1996.csv`. Only `details-1950.csv` was supplied in this chat, so the 1996 file is not fabricated or silently substituted. On a host with Internet access, run:

```bash
python3 download_dataset.py 1996
```

This downloads the latest listed NOAA/NCEI `StormEvents_details` CSV for 1996. Copy `data/details-1996.csv` to the host that will lead the 1996 DHT. The leader reads the year file; other peers need not have a local copy unless they become leader during a later rebuild.

## Compile / syntax-check

```bash
python3 -m py_compile manager.py peer.py protocol.py dht_utils.py download_dataset.py
```

No compile errors should be printed. This is Python, so no C compiler is needed.

## Start manager

On the manager host:

```bash
python3 manager.py 17500
```

Leave this terminal running.

## Start a peer

On each peer host:

```bash
python3 peer.py <MANAGER_IPV4> 17500
```

At the prompt, register it. Replace `<PEER_IPV4>` with that host's reachable IPv4 address:

```text
register falabbad <PEER_IPV4> 17501 17502
```

Use a unique alphabetic name of at most 15 letters and unique ports for each process. Example registrations:

```text
register peerone <PEERONE_IPV4> 17503 17504
register peertwo <PEERTWO_IPV4> 17505 17506
register peerthree <PEERTHREE_IPV4> 17507 17508
register peerfour <PEERFOUR_IPV4> 17509 17510
register peerfive <PEERFIVE_IPV4> 17511 17512
```

## Required full-project demo sequence

Register six peers, with five peers in the DHT and one Free peer. Ensure `details-1996.csv` exists on the host that will run the leader.

1. Compile/syntax-check the programs.
2. Run manager and six peer processes on at least four distinct end-hosts.
3. From the chosen leader (for example `falabbad`), run:

   ```text
   setup-dht falabbad 5 1996
   ```

4. From the remaining Free peer, run the required queries. The peer CLI accepts both the manager peer-name and event ID:

   ```text
   query-dht peerfive 5536849
   query-dht peerfive 2402920
   query-dht peerfive 5539287
   query-dht peerfive 55770111
   ```

   Replace `peerfive` with the peer that is actually Free after random selection. A DHT member cannot initiate `query-dht` according to the manager specification.

5. From a selected DHT member, run `leave-dht <peer-name>`. After the rebuild, the leaving peer should be Free and should be able to issue `query-dht <peer-name> <event-id>`.
6. From the other Free peer, run `join-dht <peer-name>`. After rebuild, the joiner is a DHT member and the remaining outside peer can query.
7. From the current leader, run `teardown-dht <leader-name>`.
8. For each peer now in Free state, run `dereg <peer-name>`. Stop the manager with Ctrl+C.

## Milestone 1 demo

Register three peers, then on `falabbad` run:

```text
setup-dht falabbad 3 1950
```

Expected local dataset check for the file supplied here: 223 data rows (excluding the header), so `s = first prime > 2×223 = 449`. The output must show per-node counts summing to 223 and a successful `dht-complete` manager response.

## Network notes

- Ensure the assigned UDP ports are allowed by host firewalls.
- On multi-host runs, use reachable host IPs and verify the peers can send UDP packets to each other.
- The manager listens on `0.0.0.0:<manager-port>`.
- Run one peer process per terminal; each process has exactly the required two CLI parameters (manager IP and manager port).
- All message traces are labelled `[MANAGER]` or `[PEER name]`.

## Submission checklist

- [ ] Verify every required full-project operation on the actual machines before submitting.
- [ ] Insert real version-control screenshots into the report; do not fabricate commit evidence.
- [ ] Insert the actual public/unlisted YouTube URL and accurate timestamps after recording the unedited demo.
- [ ] Include the design document PDF, source code, and documentation in the course-required ZIP archive.
