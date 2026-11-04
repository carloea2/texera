import json
import argparse
from pathlib import Path
import run_arrow_batch_bench as driver

BENCH=driver.BENCH
parser=argparse.ArgumentParser()
parser.add_argument('--primary-results',default='full-results')
parser.add_argument('--results-name',default='supplemental-memory')
args=parser.parse_args()
for name in (args.primary_results,args.results_name):
    if not name.replace('-','').replace('_','').isalnum():raise ValueError('Use simple results-directory names')
full=BENCH/args.primary_results
env=json.loads((full/'environment.json').read_text(encoding='utf-8'))
assert 'finished_at_utc' in env,'Wait for the primary benchmark to finish'
expected={}
for line in (full/'raw.jsonl').read_text(encoding='utf-8').splitlines():
    row=json.loads(line)
    if row['measurement']=='timing':expected[(row['kind'],row['workload'],row['batch_size'])]=row['fingerprint']
driver.RESULTS=BENCH/args.results_name
driver.RESULTS.mkdir(exist_ok=True)
if (driver.RESULTS/'raw.jsonl').exists():raise RuntimeError('Supplemental samples already exist; preserving them')
cells=[('original','identity',10000)]+[(kind,'identity',batch) for kind in ('strings','numeric','mixed') for batch in (1024,4096)]
cpu=env['cpu_affinity'][0]
count=0
for kind,workload,batch in cells:
    for repeat in range(2):
        for path in driver.PATHS if repeat%2==0 else list(reversed(driver.PATHS)):
            r=driver.memory('rss',kind,workload,batch,path,cpu,repeat)
            assert r['fingerprint']==expected[(kind,workload,batch)]
            count+=1
            print(f'SUPPLEMENT {count}/42 {kind}/b{batch}/{path}: RSS delta {r["rss_delta"]/1048576:.2f} MiB',flush=True)
for path in driver.PATHS:
    r=driver.memory('python_memory','original','identity',10000,path,cpu,0)
    assert r['fingerprint']==expected[('original','identity',10000)]
    print(f'ORIGINAL ALLOC {path}: {r["python_peak"]/1048576:.2f} MiB',flush=True)
print('SUPPLEMENT DONE',flush=True)
