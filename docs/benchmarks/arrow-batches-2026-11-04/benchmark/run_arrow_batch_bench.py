import argparse
import hashlib
import json
import os
import platform
import queue
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
import psutil

REPO=Path(r'C:\Users\carlo\texera\texera-worktrees\audit-pyamber-arrow')
BENCH=REPO/'target/arrow-batch-bench-20261104'
PYTHON=Path(r'C:\Users\carlo\texera\venv312\Scripts\python.exe')
CHILD=BENCH/'bench_arrow_batches.py'
RESULTS=BENCH/'results'
RESULTS.mkdir(parents=True,exist_ok=True)
PATHS=['current','arrow_pandas','pure_arrow']

def log(obj):
    with (RESULTS/'raw.jsonl').open('a',encoding='utf-8') as f:f.write(json.dumps(obj)+'\n')

def command(mode,kind,workload,batch_size,rows=10000,path=None,repeats=5):
    cmd=[str(PYTHON),str(CHILD),'--mode',mode,'--kind',kind,'--workload',workload,'--batch-size',str(batch_size),'--rows',str(rows),'--repeats',str(repeats)]
    if path:cmd+=['--path',path]
    return cmd

def launch(cmd,cpu):
    env=os.environ.copy()
    env.update(PYTHONUNBUFFERED='1',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1')
    p=subprocess.Popen(cmd,cwd=REPO,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
    proc=psutil.Process(p.pid)
    proc.cpu_affinity([cpu])
    return p,proc

def timed(kind,workload,batch_size,cpu,repeats):
    p,proc=launch(command('time',kind,workload,batch_size,repeats=repeats),cpu)
    stdout,stderr=p.communicate(timeout=240)
    if p.returncode:raise RuntimeError(f'{kind}/{workload}/{batch_size}: {stderr[-5000:]}')
    result=json.loads(stdout.strip().splitlines()[-1])
    result.update(measurement='timing',cpu=cpu)
    log(result)
    return result

def memory(mode,kind,workload,batch_size,path,cpu,repeat):
    p,proc=launch(command(mode,kind,workload,batch_size,path=path),cpu)
    line=p.stdout.readline()
    if not line:
        raise RuntimeError(p.stderr.read()[-5000:])
    ready=json.loads(line)
    if ready['state']!='ready':raise RuntimeError(ready)
    peak=ready['baseline_rss']
    samples=0
    done_queue=queue.Queue()
    reader=threading.Thread(target=lambda:done_queue.put(p.stdout.readline()),daemon=True)
    reader.start()
    p.stdin.write('GO\n');p.stdin.flush()
    deadline=time.monotonic()+180
    while reader.is_alive():
        if time.monotonic()>deadline:
            p.kill();raise TimeoutError('memory child exceeded 180s')
        try:peak=max(peak,proc.memory_info().rss)
        except psutil.NoSuchProcess:break
        samples+=1
        time.sleep(.001)
    reader.join()
    done_line=done_queue.get()
    if not done_line:raise RuntimeError(p.stderr.read()[-5000:])
    done=json.loads(done_line)
    peak=max(peak,done['end_rss'])
    p.stdin.write('CHECK\n');p.stdin.flush()
    checked=json.loads(p.stdout.readline())
    stdout,stderr=p.communicate(timeout=120)
    if p.returncode:raise RuntimeError(stderr[-5000:])
    result={**done,'measurement':mode,'repeat':repeat,'cpu':cpu,'sampled_peak_rss':peak,'rss_delta':peak-ready['baseline_rss'],'rss_samples':samples,'fingerprint':checked['fingerprint']}
    log(result)
    return result

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    global RESULTS
    p=argparse.ArgumentParser()
    p.add_argument('--repeats',type=int,default=5)
    p.add_argument('--rss-repeats',type=int,default=2)
    p.add_argument('--smoke',action='store_true')
    p.add_argument('--results-name',default='results')
    args=p.parse_args()
    if not args.results_name.replace('-','').replace('_','').isalnum():raise ValueError('results-name must be a simple directory name')
    RESULTS=BENCH/args.results_name
    RESULTS.mkdir(parents=True,exist_ok=True)
    raw=RESULTS/'raw.jsonl'
    if raw.exists():raise RuntimeError(f'Existing results preserved at {raw}; use a separate results directory for a rerun.')
    available=psutil.Process().cpu_affinity()
    load=psutil.cpu_percent(interval=.5,percpu=True)
    cpu=min(available,key=lambda c:load[c])
    head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    source_paths=[REPO/'amber/src/main/python'/p for p in ['core/models/operator.py','core/models/tuple.py','core/models/table.py','core/architecture/packaging/input_manager.py','core/architecture/packaging/output_manager.py']]
    env={'repository':str(REPO),'head':head,'platform':platform.platform(),'machine':platform.machine(),'logical_cpus':psutil.cpu_count(),'physical_cpus':psutil.cpu_count(logical=False),'total_ram':psutil.virtual_memory().total,'cpu_affinity':[cpu],'initial_cpu_load':load,'timing_repeats':args.repeats,'warmups_per_path':1,'rss_repeats':args.rss_repeats,'rss_sampling_target_ms':1,'source_sha256':{str(x.relative_to(REPO)):sha(x) for x in source_paths},'harness_sha256':sha(CHILD),'started_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
    (RESULTS/'environment.json').write_text(json.dumps(env,indent=2),encoding='utf-8')
    if args.smoke:
        cells=[('original','identity',10000),('mixed','filter',4096)]
    else:
        cells=[('original','identity',10000)]+[(kind,workload,b) for kind in ('strings','numeric','mixed') for workload in ('identity','filter','aggregate','ipc_sink') for b in (1024,4096,10000)]
    timings={}
    for i,(kind,workload,b) in enumerate(cells,1):
        r=timed(kind,workload,b,cpu,args.repeats)
        timings[(kind,workload,b)]=r
        med={path:round(statistics.median(s['ms'] for s in r['samples'] if s['path']==path),3) for path in PATHS}
        print(f'TIME {i}/{len(cells)} {kind}/{workload}/b{b} {med}',flush=True)
    memory_cells=[('mixed','filter')] if args.smoke else [(kind,workload) for kind in ('strings','numeric','mixed') for workload in ('identity','filter','aggregate','ipc_sink')]
    total=len(memory_cells)*len(PATHS)*args.rss_repeats
    count=0
    for kind,workload in memory_cells:
        expected=timings[(kind,workload,10000 if not args.smoke else 4096)]['fingerprint']
        b=10000 if not args.smoke else 4096
        for repeat in range(args.rss_repeats):
            order=PATHS if repeat%2==0 else list(reversed(PATHS))
            for path in order:
                r=memory('rss',kind,workload,b,path,cpu,repeat)
                if r['fingerprint']!=expected:raise RuntimeError(f'Memory output differs for {kind}/{workload}/{path}')
                count+=1
                print(f'RSS {count}/{total} {kind}/{workload}/{path}: delta {r["rss_delta"]/1048576:.2f} MiB',flush=True)
    kinds=['mixed'] if args.smoke else ['strings','numeric','mixed']
    for kind in kinds:
        for path in PATHS:
            r=memory('python_memory',kind,'identity',10000,path,cpu,0)
            if not args.smoke and r['fingerprint']!=timings[(kind,'identity',10000)]['fingerprint']:
                raise RuntimeError('Python allocation output differs')
            print(f'ALLOC {kind}/{path}: Python peak {r["python_peak"]/1048576:.2f} MiB; Arrow pool {r["arrow_pool_peak"]/1048576:.2f} MiB',flush=True)
    env['finished_at_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    env['final_cpu_load']=psutil.cpu_percent(interval=.5,percpu=True)
    env['source_sha256_after']={str(x.relative_to(REPO)):sha(x) for x in source_paths}
    assert env['source_sha256']==env['source_sha256_after'],'Engine source changed during measurement'
    (RESULTS/'environment.json').write_text(json.dumps(env,indent=2),encoding='utf-8')
    print('DONE',str(RESULTS),flush=True)

if __name__=='__main__':main()
