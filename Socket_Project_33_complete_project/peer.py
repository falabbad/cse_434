#!/usr/bin/env python3
"""Interactive DHT peer. Full-project manager and peer protocols for CSE 434."""
import argparse, ipaddress, random, socket, sys, threading, time, traceback
from pathlib import Path
from protocol import encode, decode, valid_port, PORT_MIN, PORT_MAX
from dht_utils import load_year_csv, prime_table_size, hash_record, bucket_add, bucket_find, count_table

class Peer:
    def __init__(self, manager_ip, manager_port):
        self.manager_ip=str(ipaddress.IPv4Address(manager_ip)); self.manager_port=int(manager_port)
        if not valid_port(self.manager_port): raise ValueError(f"Manager port must be in {PORT_MIN}-{PORT_MAX}")
        self.name=None; self.ip=None; self.m_port=None; self.p_port=None
        self.msock=None; self.psock=None; self.listener=None; self.stop_event=threading.Event()
        self.lock=threading.RLock(); self.waiters={}; self.members=[]; self.node_id=None; self.ring_size=0; self.right=None
        self.year=None; self.table_size=0; self.table={}; self.dht_ready=False; self.registered=False
        self.count_event=threading.Event(); self.count_result=None; self.store_event=threading.Event(); self.store_ack_event=threading.Event(); self.store_acks=set(); self.seen_store_ids=set(); self.teardown_event=threading.Event()
        self.setid_acks=set(); self.setid_event=threading.Event(); self.rebuild_event=threading.Event(); self.rebuild_notice=None
        self.query_events={}; self.pending_join=None
    def log(self,msg): print(f"[PEER {self.name or 'unregistered'}] {msg}",flush=True)
    def send_manager(self,msg,timeout=15):
        if not self.msock: raise RuntimeError("Register first")
        self.log(f"TX MANAGER {msg.get('command')} -> {self.manager_ip}:{self.manager_port}")
        self.msock.sendto(encode(msg),(self.manager_ip,self.manager_port)); self.msock.settimeout(timeout)
        try:
            data,addr=self.msock.recvfrom(65507); resp=decode(data)
            self.log(f"RX MANAGER {resp.get('code')} ({resp.get('message','')})")
            return resp
        finally: self.msock.settimeout(None)
    def bind_register(self,name,ip,mp,pp):
        if not name.isalpha() or not 1 <= len(name) <= 15: raise ValueError("Peer name must be alphabetic and 1-15 characters")
        ip=str(ipaddress.IPv4Address(ip)); mp=int(mp); pp=int(pp)
        if not valid_port(mp) or not valid_port(pp) or mp==pp: raise ValueError(f"Peer ports must be distinct and in {PORT_MIN}-{PORT_MAX}")
        ms=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); ps=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        ms.bind(("0.0.0.0",mp)); ps.bind(("0.0.0.0",pp))
        self.name,self.ip,self.m_port,self.p_port=name,ip,mp,pp; self.msock,self.psock=ms,ps
        self.msock.settimeout(15)
        resp=self.send_manager({'command':'register','peer_name':name,'ip':ip,'m_port':mp,'p_port':pp})
        if resp.get('code')!='SUCCESS':
            ms.close(); ps.close(); self.msock=self.psock=None; self.name=None
            raise RuntimeError(resp.get('message','Registration failed'))
        self.registered=True; self.msock.settimeout(None)
        self.listener=threading.Thread(target=self.listen_p2p,daemon=True); self.listener.start()
        self.log(f"REGISTERED at {ip}; m-port={mp}; p-port={pp}")
    def send_peer(self,member,msg):
        addr=(member['ip'],int(member['p_port']))
        if msg.get('type') not in ('store','store-ack'):
            self.log(f"TX P2P {msg.get('type')} -> {member.get('peer_name')} {addr}")
        self.psock.sendto(encode(msg),addr)
    def send_addr(self,addr,msg):
        if msg.get('type') not in ('store','store-ack'):
            self.log(f"TX P2P {msg.get('type')} -> {addr}")
        self.psock.sendto(encode(msg),addr)
    def member_by_name(self,name):
        for m in self.members:
            if m['peer_name']==name:return m
        return None
    def current_member(self): return self.member_by_name(self.name)
    def set_members(self,members,node_id=None):
        with self.lock:
            self.members=list(members); self.ring_size=len(self.members)
            if node_id is None:
                for i,m in enumerate(self.members):
                    if m['peer_name']==self.name: node_id=i; break
            self.node_id=node_id
            if self.members and node_id is not None: self.right=self.members[(node_id+1)%len(self.members)]
            else: self.right=None
    def listen_p2p(self):
        while not self.stop_event.is_set():
            try:
                data,addr=self.psock.recvfrom(65507); m=decode(data)
                if m.get('type') not in ('store','store-ack'):
                    self.log(f"RX P2P {m.get('type')} from {addr}")
                try: self.handle_p2p(m,addr)
                except Exception as e: self.log(f"P2P HANDLER ERROR: {e}"); traceback.print_exc()
            except OSError:
                if self.stop_event.is_set(): break
            except Exception as e: self.log(f"Bad P2P datagram: {e}")
    def handle_p2p(self,m,addr):
        typ=m.get('type')
        if typ=='set-id':
            members=m['members']; nid=int(m['node_id']); self.year=int(m.get('year',self.year or 1950))
            self.set_members(members,nid); self.dht_ready=False; self.table={}; self.table_size=0; self.seen_store_ids=set()
            self.send_addr(addr,{'type':'set-id-ack','peer_name':self.name,'node_id':nid})
        elif typ=='set-id-ack':
            with self.lock:
                self.setid_acks.add(int(m['node_id']))
                if len(self.setid_acks)>=max(0,self.ring_size-1): self.setid_event.set()
        elif typ=='store':
            if int(m['destination_id'])==self.node_id:
                sid=str(m.get('store_id',''))
                if sid not in self.seen_store_ids:
                    bucket_add(self.table,int(m['position']),m['record']); self.seen_store_ids.add(sid)
                if m.get('return_addr'):
                    self.send_addr(tuple(m['return_addr']),{'type':'store-ack','store_id':sid})
            else:
                if not self.right: self.log('STORE cannot forward: no right neighbour'); return
                self.send_peer(self.right,m)
        elif typ=='store-ack':
            with self.lock:
                self.store_acks.add(str(m.get('store_id')))
                self.store_ack_event.set()
        elif typ=='store-complete':
            if m.get('origin')==self.name: self.store_event.set()
            elif self.right: self.send_peer(self.right,m)
        elif typ=='count-report':
            counts=m.get('counts',{})
            if self.name==m.get('origin'):
                counts[str(self.node_id)]=count_table(self.table); self.count_result=counts; self.count_event.set()
            else:
                counts[str(self.node_id)]=count_table(self.table)
                if self.right: self.send_peer(self.right,{**m,'counts':counts})
        elif typ=='find-event': self.handle_find_event(m)
        elif typ=='find-event-response':
            qid=m.get('query_id'); ev=self.query_events.get(qid)
            if ev is not None: ev['result']=m; ev['event'].set()
        elif typ=='teardown':
            self.table={}; self.table_size=0; self.dht_ready=False
            if m.get('origin')==self.name: self.teardown_event.set()
            elif self.right: self.send_peer(self.right,m)
        elif typ=='reset-id':
            members=m['members']; cursor=int(m['cursor']); self.year=int(m.get('year',self.year or 1950))
            self.set_members(members,cursor); self.table={}; self.table_size=0; self.dht_ready=False
            nxt=cursor+1
            if nxt<len(members): self.send_peer(members[nxt],{**m,'cursor':nxt})
            else: self.send_addr(tuple(m['leave_return']),{'type':'reset-complete','members':members,'year':self.year})
        elif typ=='reset-complete':
            self.rebuild_notice=m; self.rebuild_event.set()
        elif typ=='rebuild-dht':
            threading.Thread(target=self._rebuild_worker,args=(m,),daemon=True).start()
        elif typ=='rebuild-complete':
            self.rebuild_notice=m; self.rebuild_event.set()
        elif typ=='join-request':
            if self.name!=m.get('leader'):
                self.log('Ignoring join request delivered to non-leader'); return
            threading.Thread(target=self.handle_join_request,args=(m,),daemon=True).start()
        elif typ=='join-rebuild-done':
            self.rebuild_notice=m; self.rebuild_event.set()
        elif typ=='ping': self.send_addr(addr,{'type':'pong','name':self.name})
        else: self.log(f"Unknown P2P type: {typ}")
    def init_dht(self,response):
        self.year=int(response['year']); self.set_members(response['peers'],0); self.build_dht()
    def build_dht(self,notify=None):
        # node 0 must be this process; set-id is sent directly per the specification.
        if not self.members or self.members[0]['peer_name']!=self.name:
            raise RuntimeError('Only node 0/current leader may populate the DHT')
        self.set_members(self.members,0); self.table={}; self.dht_ready=False; self.seen_store_ids=set(); self.store_acks=set()
        self.setid_acks=set(); self.setid_event.clear()
        for i,member in enumerate(self.members[1:],start=1):
            self.send_peer(member,{'type':'set-id','node_id':i,'ring_size':len(self.members),'members':self.members,'year':self.year})
        deadline=time.time()+20
        while len(self.setid_acks)<len(self.members)-1 and time.time()<deadline: self.setid_event.wait(.25)
        if len(self.setid_acks)<len(self.members)-1: raise TimeoutError(f"SET-ID acknowledgements missing: {self.setid_acks}")
        records,fields,path=load_year_csv(self.year); self.table_size=prime_table_size(records)
        self.log(f"Loading {len(records)} records from {path}; table size={self.table_size}; ring size={len(self.members)}")
        pending_messages={}
        self.log(f"Sending {len(records)} records through the ring; local table size={self.table_size}")
        for index,rec in enumerate(records):
            pos,dest=hash_record(rec['event_id'],self.table_size,len(self.members))
            if dest==0:
                bucket_add(self.table,pos,rec)
            else:
                sid=str(index)
                payload={'type':'store','origin':self.name,'store_id':sid,'position':pos,'destination_id':dest,'record':rec,'return_addr':[self.ip,self.p_port]}
                pending_messages[sid]=payload
                self.send_peer(self.right,payload)
        # UDP can drop or reorder datagrams. Destination acknowledgements and idempotent store IDs
        # make the load phase reliable without ever bypassing the logical ring.
        deadline=time.time()+120; last_retry=time.time()
        while len(self.store_acks)<len(pending_messages) and time.time()<deadline:
            self.store_ack_event.wait(.25); self.store_ack_event.clear()
            if time.time()-last_retry>=2.0:
                missing=set(pending_messages)-self.store_acks
                for sid in missing: self.send_peer(self.right,pending_messages[sid])
                last_retry=time.time()
        if len(self.store_acks)<len(pending_messages):
            raise TimeoutError(f"STORE acknowledgements missing: {len(pending_messages)-len(self.store_acks)}")
        self.store_event.clear(); self.send_peer(self.right,{'type':'store-complete','origin':self.name})
        if not self.store_event.wait(60): raise TimeoutError('STORE-COMPLETE did not return around ring')
        self.count_event.clear(); self.count_result=None
        self.send_peer(self.right,{'type':'count-report','origin':self.name,'counts':{}})
        if not self.count_event.wait(20): raise TimeoutError('COUNT-REPORT did not return around ring')
        counts={int(k):int(v) for k,v in self.count_result.items()}
        total=sum(counts.values()); self.log('========== FINAL DHT RECORD COUNTS ==========')
        for i in range(len(self.members)): self.log(f"Node {i}: {self.members[i]['peer_name']} -> {counts.get(i,0)} records")
        self.log(f"TOTAL STORED RECORDS: {total}"); self.log('============================================')
        if total!=len(records): raise RuntimeError(f"Record-count mismatch: expected {len(records)}, stored {total}")
        self.dht_ready=True
        if notify:
            self.send_addr(tuple(notify),{'type':'rebuild-complete','leader':self.name,'members':self.members,'year':self.year,'table_size':self.table_size})
        self.log('DHT build/rebuild complete.')
    def handle_find_event(self,m):
        event_id=int(m['event_id']); table_size=int(m['table_size']); pos,dest=hash_record(event_id,table_size,int(m['ring_size']))
        found=bucket_find(self.table,pos,event_id)
        seq=list(m.get('id_seq',[])); visited=set(int(x) for x in m.get('visited',[]))
        if self.node_id not in visited:
            visited.add(self.node_id); seq.append(self.node_id)
        if found is not None:
            result={'type':'find-event-response','query_id':m['query_id'],'code':'SUCCESS','event_id':event_id,'record':found,'id_seq':seq}
            self.send_addr(tuple(m['return_addr']),result); return
        all_ids=set(range(self.ring_size)); remaining=list(all_ids-visited)
        if not remaining:
            self.send_addr(tuple(m['return_addr']),{'type':'find-event-response','query_id':m['query_id'],'code':'FAILURE','event_id':event_id,'id_seq':seq}); return
        next_id=random.choice(remaining); nxt=self.members[next_id]
        self.send_peer(nxt,{**m,'visited':sorted(visited),'id_seq':seq})
    def cmd_setup(self,args):
        if len(args)!=3: print('Usage: setup-dht <peer-name> <n> <YYYY>'); return
        name,n,year=args[0],int(args[1]),int(args[2]); resp=self.send_manager({'command':'setup-dht','peer_name':name,'n':n,'year':year})
        if resp.get('code')=='SUCCESS':
            self.init_dht(resp)
            done=self.send_manager({'command':'dht-complete','peer_name':self.name,'table_size':self.table_size})
            if done.get('code')=='SUCCESS': self.log('Manager confirmed DHT creation is complete.')
    def cmd_query(self,args):
        if len(args)!=2: print('Usage: query-dht <peer-name> <event-id>'); return
        name,event_id=args[0],int(args[1]); resp=self.send_manager({'command':'query-dht','peer_name':name})
        if resp.get('code')!='SUCCESS': return
        start=resp['peer']; qid=f"{self.name}-{time.time_ns()}"; ev=threading.Event(); self.query_events[qid]={'event':ev,'result':None}
        msg={'type':'find-event','query_id':qid,'event_id':event_id,'table_size':self.table_size or 0,'ring_size':len(resp.get('members',self.members)) or self.ring_size,
             'return_addr':[self.ip,self.p_port],'visited':[],'id_seq':[]}
        # Query metadata is supplied by the manager in response to keep Free peers independent of local DHT state.
        msg['table_size']=int(resp.get('table_size',self.table_size or 449)); msg['ring_size']=int(resp.get('ring_size',self.ring_size or 3))
        self.send_peer(start,msg)
        if not ev.wait(60): self.log('Query timed out'); self.query_events.pop(qid,None); return
        result=self.query_events.pop(qid)['result']; self.log(f"QUERY RESULT {result['code']}; id-seq={result.get('id_seq',[])}")
        if result['code']=='SUCCESS':
            for k,v in result['record']['fields'].items(): print(f"{k}: {v}")
        else: print(f"Storm event {event_id} not found in the DHT.")
    def cmd_leave(self,args):
        if len(args)!=1: print('Usage: leave-dht <peer-name>'); return
        resp=self.send_manager({'command':'leave-dht','peer_name':args[0]})
        if resp.get('code')!='SUCCESS': return
        old=list(resp['members']); self.year=int(resp['year']); self.set_members(old)
        if self.name not in [x['peer_name'] for x in old]: self.log('This process is not in returned DHT members'); return
        self.teardown_event.clear(); self.send_peer(self.right,{'type':'teardown','origin':self.name})
        if not self.teardown_event.wait(30): raise TimeoutError('Teardown did not return during leave')
        idx=next(i for i,x in enumerate(old) if x['peer_name']==self.name)
        new_members=old[idx+1:]+old[:idx]
        if len(new_members)<2: raise RuntimeError('Leave would leave fewer than two nodes; project DHT minimum is three at setup')
        self.rebuild_event.clear(); first=new_members[0]
        self.send_peer(first,{'type':'reset-id','members':new_members,'cursor':0,'year':self.year,'leave_return':[self.ip,self.p_port]})
        if not self.rebuild_event.wait(30): raise TimeoutError('RESET-ID did not complete')
        self.rebuild_event.clear(); self.send_peer(new_members[0],{'type':'rebuild-dht','members':new_members,'year':self.year,'notify':[self.ip,self.p_port]})
        if not self.rebuild_event.wait(120): raise TimeoutError('Rebuild completion not received')
        newleader=new_members[0]['peer_name']; r=self.send_manager({'command':'dht-rebuilt','peer_name':self.name,'new_leader':newleader,'table_size':int((self.rebuild_notice or {}).get('table_size',0))})
        if r.get('code')=='SUCCESS': self.members=[]; self.node_id=None; self.right=None; self.dht_ready=False; self.table={}; self.log(f"Leave complete; new leader is {newleader}; this peer is Free")
    def _rebuild_worker(self,m):
        try:
            self.members=m['members']; self.year=int(m['year']); self.set_members(self.members,0); self.build_dht(notify=m.get('notify'))
        except Exception as e: self.log(f'Rebuild worker failed: {e}')
    def handle_join_request(self,m):
        old=list(self.members); joiner=m['joiner']; year=int(m['year'])
        self.teardown_event.clear(); self.send_peer(self.right,{'type':'teardown','origin':self.name})
        if not self.teardown_event.wait(30): raise TimeoutError('Teardown did not return for join')
        new_members=old+[joiner]; self.year=year; self.set_members(new_members,0)
        self.build_dht(notify=[joiner['ip'],joiner['p_port']])
    def cmd_join(self,args):
        if len(args)!=1: print('Usage: join-dht <peer-name>'); return
        resp=self.send_manager({'command':'join-dht','peer_name':args[0]})
        if resp.get('code')!='SUCCESS': return
        leader_name=resp['leader']; leader=next(x for x in resp['members'] if x['peer_name']==leader_name)
        self.rebuild_event.clear(); self.send_peer(leader,{'type':'join-request','leader':leader_name,'joiner':{'peer_name':self.name,'ip':self.ip,'p_port':self.p_port},'year':int(resp['year'])})
        if not self.rebuild_event.wait(180): raise TimeoutError('Join rebuild completion not received')
        r=self.send_manager({'command':'dht-rebuilt','peer_name':self.name,'new_leader':leader_name,'table_size':int((self.rebuild_notice or {}).get('table_size',0))})
        if r.get('code')=='SUCCESS': self.members=self.rebuild_notice.get('members',[]); self.set_members(self.members); self.dht_ready=True; self.log(f"Join complete; DHT leader={leader_name}")
    def cmd_teardown(self,args):
        if len(args)!=1: print('Usage: teardown-dht <peer-name>'); return
        resp=self.send_manager({'command':'teardown-dht','peer_name':args[0]})
        if resp.get('code')!='SUCCESS': return
        self.members=resp['members']; self.year=int(resp['year']); self.set_members(self.members,0)
        self.teardown_event.clear(); self.send_peer(self.right,{'type':'teardown','origin':self.name})
        if not self.teardown_event.wait(30): raise TimeoutError('Teardown did not return')
        r=self.send_manager({'command':'teardown-complete','peer_name':self.name})
        if r.get('code')=='SUCCESS': self.members=[]; self.node_id=None; self.right=None; self.dht_ready=False; self.table={}; self.log('DHT deleted; all member peers are Free')
    def execute(self,line):
        parts=line.strip().split()
        if not parts:return True
        cmd,args=parts[0],parts[1:]
        try:
            if cmd=='register':
                if len(args)!=4: print('Usage: register <peer-name> <IPv4> <m-port> <p-port>'); return True
                self.bind_register(args[0],args[1],int(args[2]),int(args[3]))
            elif not self.registered: print('Register first.');
            elif cmd=='setup-dht': self.cmd_setup(args)
            elif cmd=='query-dht': self.cmd_query(args)
            elif cmd=='leave-dht': self.cmd_leave(args)
            elif cmd=='join-dht': self.cmd_join(args)
            elif cmd=='teardown-dht': self.cmd_teardown(args)
            elif cmd=='dereg':
                if len(args)!=1: print('Usage: dereg <peer-name>'); return True
                resp=self.send_manager({'command':'deregister','peer_name':args[0]})
                if resp.get('code')=='SUCCESS': self.close(); return False
            elif cmd=='help': print('register NAME IP MPORT PPORT | setup-dht NAME N YEAR | query-dht NAME EVENT_ID | leave-dht NAME | join-dht NAME | teardown-dht NAME | dereg NAME | help | exit')
            elif cmd=='exit': self.close(); return False
            else: print('Unknown command. Type help.')
        except Exception as e: self.log(f"ERROR: {e}"); traceback.print_exc()
        return True
    def close(self):
        self.stop_event.set()
        for s in (self.msock,self.psock):
            try:
                if s: s.close()
            except OSError: pass

def main():
    ap=argparse.ArgumentParser(description='CSE434 DHT peer'); ap.add_argument('manager_ip'); ap.add_argument('manager_port',type=int); args=ap.parse_args()
    peer=Peer(args.manager_ip,args.manager_port)
    print('Commands: register NAME IP MPORT PPORT | setup-dht NAME N YEAR | query-dht NAME EVENT_ID | leave-dht NAME | join-dht NAME | teardown-dht NAME | dereg NAME | help | exit',flush=True)
    try:
        while True:
            line=input(f"{peer.name or 'peer'}> ")
            if not peer.execute(line): break
    except (EOFError,KeyboardInterrupt): print(); peer.close()
if __name__=='__main__': main()
