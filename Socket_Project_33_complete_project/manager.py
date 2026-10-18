#!/usr/bin/env python3
"""Always-on UDP manager for the CSE 434 DHT project."""
import argparse, ipaddress, random, socket, sys, threading
from protocol import encode, decode, ok_response, fail_response, valid_port, PORT_MIN, PORT_MAX

class Manager:
    def __init__(self, port):
        if not valid_port(port): raise ValueError(f"Manager port must be in {PORT_MIN}-{PORT_MAX}")
        self.port=int(port); self.sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0",self.port)); self.lock=threading.RLock()
        self.peers={}; self.dht_exists=False; self.dht_complete=False; self.leader=None
        self.members=[]; self.year=None; self.pending=None; self.table_size=0
        print(f"[MANAGER] Listening UDP 0.0.0.0:{self.port}; Group 33 ports {PORT_MIN}-{PORT_MAX}",flush=True)
    def tx(self,msg,addr):
        print(f"[MANAGER] TX {msg.get('code',msg.get('type','message'))} -> {addr}",flush=True)
        self.sock.sendto(encode(msg),addr)
    def reply(self,addr,msg): self.tx(msg,addr)
    def free_names(self): return [n for n,p in self.peers.items() if p['state']=='Free']
    def used_ports(self):
        ports={self.port}
        for p in self.peers.values(): ports.update((p['m_port'],p['p_port']))
        return ports
    def handle_register(self,m):
        if self.dht_exists: return fail_response("A DHT exists; registration is closed until teardown.")
        name=str(m.get('peer_name','')); ip=str(m.get('ip',''))
        try: ip=str(ipaddress.IPv4Address(ip)); mp=int(m.get('m_port')); pp=int(m.get('p_port'))
        except Exception: return fail_response("Invalid IPv4 address or port.")
        if not name.isalpha() or not 1 <= len(name) <= 15: return fail_response("Peer name must be alphabetic and at most 15 characters.")
        if name in self.peers: return fail_response("Peer name is already registered.")
        if not valid_port(mp) or not valid_port(pp): return fail_response(f"Ports must be in {PORT_MIN}-{PORT_MAX}.")
        if mp == pp or mp in self.used_ports() or pp in self.used_ports(): return fail_response("Ports must be unique and cannot equal the manager port.")
        self.peers[name]={'name':name,'ip':ip,'m_port':mp,'p_port':pp,'state':'Free'}
        print(f"[MANAGER] REGISTERED {name} at {ip}; m={mp}, p={pp}; state=Free",flush=True)
        return ok_response(f"{name} registered successfully.")
    def handle_setup(self,m):
        name=str(m.get('peer_name',''))
        try: n=int(m.get('n')); year=int(m.get('year'))
        except Exception: return fail_response("n and year must be integers.")
        if self.dht_exists: return fail_response("A DHT already exists.")
        if name not in self.peers: return fail_response("Leader is not registered.")
        if n < 3: return fail_response("DHT size n must be at least 3.")
        if len(self.peers) < n: return fail_response("Fewer than n peers are registered.")
        if year < 1950 or year > 2019: return fail_response("Dataset year must be between 1950 and 2019 for this project specification.")
        if self.peers[name]['state'] != 'Free': return fail_response("Requested leader is not Free.")
        candidates=[p for nm,p in self.peers.items() if nm != name and p['state']=='Free']
        if len(candidates) < n-1: return fail_response("Not enough Free peers to build the DHT.")
        random.shuffle(candidates); chosen=[self.peers[name]]+candidates[:n-1]
        self.members=[p['name'] for p in chosen]; self.leader=name; self.year=year
        self.dht_exists=True; self.dht_complete=False; self.pending=None
        for i,p in enumerate(chosen): p['state']='Leader' if i==0 else 'InDHT'
        tuples=[{'peer_name':p['name'],'ip':p['ip'],'p_port':p['p_port']} for p in chosen]
        print(f"[MANAGER] DHT setup selected (leader first): {self.members}; year={year}",flush=True)
        return ok_response("DHT setup accepted.",year=year,ring_size=n,peers=tuples)
    def handle_dht_complete(self,m):
        if not self.dht_exists or str(m.get('peer_name','')) != self.leader: return fail_response("Only the current DHT leader may send dht-complete.")
        self.dht_complete=True; self.table_size=int(m.get('table_size',self.table_size or 0))
        print(f"[MANAGER] DHT-COMPLETE confirmed from leader {self.leader}; table_size={self.table_size}",flush=True)
        return ok_response("DHT creation/rebuild completed.")
    def handle_query(self,m):
        name=str(m.get('peer_name',''))
        if not self.dht_exists or not self.dht_complete: return fail_response("DHT is not fully constructed.")
        if name not in self.peers: return fail_response("Peer is not registered.")
        if self.peers[name]['state']!='Free': return fail_response("Only a Free peer may initiate query-dht.")
        p=random.choice(self.members); x=self.peers[p]
        return ok_response("Query may start at selected DHT member.",peer={'peer_name':p,'ip':x['ip'],'p_port':x['p_port']},members=self.member_tuples(),ring_size=len(self.members),table_size=self.table_size,year=self.year)
    def handle_leave(self,m):
        name=str(m.get('peer_name',''))
        if not self.dht_exists or not self.dht_complete or name not in self.members: return fail_response("Peer is not a member of a completed DHT.")
        if self.pending: return fail_response("Another DHT modification is pending.")
        self.pending={'kind':'leave','peer':name}; self.dht_complete=False
        return ok_response("Leave accepted; rebuild protocol may begin.",members=self.member_tuples(),year=self.year,leader=self.leader)
    def handle_join(self,m):
        name=str(m.get('peer_name',''))
        if not self.dht_exists or not self.dht_complete: return fail_response("No completed DHT exists.")
        if name not in self.peers or self.peers[name]['state']!='Free': return fail_response("Joining peer must be registered and Free.")
        if self.pending: return fail_response("Another DHT modification is pending.")
        self.pending={'kind':'join','peer':name}; self.dht_complete=False
        return ok_response("Join accepted; leader will rebuild the DHT.",members=self.member_tuples(),year=self.year,leader=self.leader)
    def member_tuples(self,names=None):
        names = self.members if names is None else names
        return [{'peer_name':n,'ip':self.peers[n]['ip'],'p_port':self.peers[n]['p_port']} for n in names if n in self.peers]
    def handle_rebuilt(self,m):
        name=str(m.get('peer_name','')); newleader=str(m.get('new_leader',''))
        if not self.pending or name != self.pending['peer']: return fail_response("dht-rebuilt peer does not match pending join/leave.")
        if newleader not in self.peers: return fail_response("New leader is not registered.")
        kind=self.pending['kind']
        if kind=='leave':
            if name not in self.members or newleader not in self.members or newleader==name: return fail_response("Invalid leave rebuild membership.")
            self.members=[n for n in self.members if n!=name]
            self.peers[name]['state']='Free'
        else:
            if name in self.members: return fail_response("Joining peer is already in DHT.")
            self.members.append(name); self.peers[name]['state']='InDHT'
        self.leader=newleader
        for n in self.members: self.peers[n]['state']='Leader' if n==newleader else 'InDHT'
        self.pending=None; self.dht_complete=True; self.table_size=int(m.get('table_size',self.table_size or 0))
        print(f"[MANAGER] DHT rebuilt; leader={self.leader}; members={self.members}; table_size={self.table_size}",flush=True)
        return ok_response("DHT rebuild confirmed.")
    def handle_deregister(self,m):
        name=str(m.get('peer_name',''))
        if name not in self.peers: return fail_response("Peer is not registered.")
        if self.peers[name]['state']!='Free': return fail_response("Cannot deregister a peer in the DHT.")
        del self.peers[name]; return ok_response(f"{name} deregistered.")
    def handle_teardown(self,m):
        name=str(m.get('peer_name',''))
        if not self.dht_exists or name!=self.leader: return fail_response("Only the current leader may teardown the DHT.")
        if not self.dht_complete: return fail_response("DHT is not in a stable completed state.")
        self.dht_complete=False; self.pending={'kind':'teardown','peer':name}
        return ok_response("Teardown accepted.",members=self.member_tuples(),year=self.year)
    def handle_teardown_complete(self,m):
        name=str(m.get('peer_name',''))
        if not self.dht_exists or name!=self.leader: return fail_response("Only the leader may send teardown-complete.")
        for n in self.members:
            if n in self.peers: self.peers[n]['state']='Free'
        self.members=[]; self.leader=None; self.year=None; self.table_size=0; self.dht_exists=False; self.dht_complete=False; self.pending=None
        return ok_response("DHT torn down; members returned to Free state.")
    def dispatch(self,m):
        cmd=str(m.get('command',''))
        if self.dht_exists and not self.dht_complete and cmd not in ('dht-complete','dht-rebuilt','teardown-complete'):
            return fail_response("DHT is being constructed or modified; command rejected.")
        if cmd=='register': return self.handle_register(m)
        if cmd=='setup-dht': return self.handle_setup(m)
        if cmd=='dht-complete': return self.handle_dht_complete(m)
        if cmd=='query-dht': return self.handle_query(m)
        if cmd=='leave-dht': return self.handle_leave(m)
        if cmd=='join-dht': return self.handle_join(m)
        if cmd=='dht-rebuilt': return self.handle_rebuilt(m)
        if cmd=='deregister': return self.handle_deregister(m)
        if cmd=='teardown-dht': return self.handle_teardown(m)
        if cmd=='teardown-complete': return self.handle_teardown_complete(m)
        return fail_response(f"Unknown command: {cmd}")
    def run(self):
        try:
            while True:
                data,addr=self.sock.recvfrom(65507)
                try:
                    m=decode(data); print(f"[MANAGER] RX {m.get('command')} from {addr}",flush=True)
                    with self.lock: resp=self.dispatch(m)
                    # replies go to registered m-port when known; registration response goes to sender port
                    name=m.get('peer_name'); target=addr
                    if name in self.peers: target=(self.peers[name]['ip'],self.peers[name]['m_port'])
                    self.reply(target,resp)
                except Exception as e:
                    print(f"[MANAGER] ERROR: {e}",flush=True)
                    try: self.reply(addr,fail_response(str(e)))
                    except Exception: pass
        except KeyboardInterrupt: print("\n[MANAGER] Stopped by operator.",flush=True)
        finally: self.sock.close()

def main():
    ap=argparse.ArgumentParser(description="CSE434 DHT manager"); ap.add_argument('manager_port',type=int); args=ap.parse_args()
    try: Manager(args.manager_port).run()
    except Exception as e: print(f"[MANAGER] Fatal: {e}",file=sys.stderr); sys.exit(2)
if __name__=='__main__': main()
