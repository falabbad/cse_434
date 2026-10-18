#!/usr/bin/env python3
"""Download a year-specific NOAA Storm Events details CSV into data/."""
import argparse, gzip, re, shutil, urllib.request
from pathlib import Path
BASE='https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('year',type=int,help='Storm-data year, e.g. 1996'); args=ap.parse_args()
    if not 1950 <= args.year <= 2019: raise SystemExit('This course project supports years 1950-2019.')
    print(f'Reading NOAA index: {BASE}')
    req=urllib.request.Request(BASE,headers={'User-Agent':'CSE434-student-project/1.0'})
    with urllib.request.urlopen(req,timeout=30) as r: html=r.read().decode('utf-8','replace')
    pat=rf'(StormEvents_details-ftp_v1\.0_d{args.year}_c(\d{{8}})\.csv\.gz)'
    matches=re.findall(pat,html)
    if not matches: raise SystemExit(f'No NOAA details CSV archive found for {args.year} at {BASE}')
    filename=max(matches,key=lambda x:x[1])[0]
    url=BASE+filename; data_dir=Path(__file__).resolve().parent/'data'; data_dir.mkdir(exist_ok=True)
    gz_path=data_dir/(filename)
    print(f'Downloading {url}')
    req=urllib.request.Request(url,headers={'User-Agent':'CSE434-student-project/1.0'})
    with urllib.request.urlopen(req,timeout=120) as response, gz_path.open('wb') as out: shutil.copyfileobj(response,out)
    csv_path=data_dir/f'details-{args.year}.csv'
    with gzip.open(gz_path,'rb') as src, csv_path.open('wb') as dst: shutil.copyfileobj(src,dst)
    print(f'Saved {csv_path} ({csv_path.stat().st_size:,} bytes)')
    print('The downloaded file is the official NOAA/NCEI details dataset. Check your course instructions if the instructor supplied a specific snapshot.')
if __name__=='__main__': main()
