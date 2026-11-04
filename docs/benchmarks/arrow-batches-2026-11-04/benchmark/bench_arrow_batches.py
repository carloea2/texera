"""Local conversion/UDF benchmark. Direct paths are benchmark prototypes, not engine features."""
import os
os.environ.update(OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1')
import argparse
import gc
import hashlib
import json
import statistics
import sys
import time
import tracemalloc
from pathlib import Path

REPO=Path(r'C:\Users\carlo\texera\texera-worktrees\audit-pyamber-arrow')
BENCH=REPO/'target/arrow-batch-bench-20261104'
sys.path[:0]=[str(BENCH/'generated'),str(REPO/'amber/src/main/python')]
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import psutil
from core.models import Schema, Tuple, ArrowTableTupleProvider, BatchOperator
from core.models.table import Table, all_output_to_tuple
from core.models.payload import DataFrame
from core.architecture.packaging.input_manager import InputManager
from core.architecture.packaging.output_manager import OutputManager
from proto.org.apache.texera.amber.core import PortIdentity, ChannelIdentity, ActorVirtualIdentity

pa.set_cpu_count(1)
pa.set_io_thread_count(1)
PATHS=['current','arrow_pandas','pure_arrow']

def dataset(kind,n):
    i=np.arange(n,dtype=np.int64)
    null=(i%17)==0
    if kind=='original':
        return pa.table({f'c{k}':['x'*64]*n for k in range(10)})
    if kind=='strings':
        values={}
        for k in range(10):
            strings=[(('A' if j%2==0 else 'B')+f'{k}-{j%64:02d}-'+'x'*64)[:64] for j in range(n)]
            values[f'c{k}']=pa.array(strings,type=pa.string(),mask=null if k in (0,1,4) else None)
    elif kind=='numeric':
        values={f'c{k}':pa.array(i%16 if k==0 else (i*(k+1) if k%2==0 else i*(k+1)/8),type=pa.int64() if k%2==0 else pa.float64(),mask=null if k in (0,1,4) else None) for k in range(10)}
    elif kind=='mixed':
        values={
            'c0':pa.array([f'k{j%16:02d}' for j in range(n)],type=pa.string(),mask=null),
            'c1':pa.array(i,type=pa.int64(),mask=null),
            'c2':pa.array(i/8,type=pa.float64()),
            'c3':pa.array((np.datetime64('2026-01-01','ms')+i.astype('timedelta64[ms]')),type=pa.timestamp('ms')),
            'c4':pa.array([(f'{j%64:02d}-'+'x'*64)[:64] for j in range(n)],type=pa.string(),mask=null),
            'c5':pa.array(i%2==0,type=pa.bool_(),mask=null),
            'c6':pa.array(i%1000,type=pa.int32()),
            'c7':pa.array(i/16,type=pa.float64(),mask=null),
            'c8':pa.array((np.datetime64('2026-01-01','ms')+i.astype('timedelta64[ms]')),type=pa.timestamp('ms'),mask=null),
            'c9':pa.array([f'café-{j%32}' for j in range(n)],type=pa.string()),
        }
    else:
        raise ValueError(kind)
    table=pa.table(values)
    # Canonical engine types, including its timestamp precision and field metadata.
    return table.cast(Schema(arrow_schema=table.schema).as_arrow_schema(),safe=True)

def pandas_work(frame,kind,workload):
    if workload in ('identity','ipc_sink'):
        return frame
    if workload=='filter':
        if kind=='strings':mask=frame.c0.str.startswith('A',na=False)
        elif kind=='numeric':mask=frame.c0.lt(8).fillna(False)
        else:mask=frame.c6.lt(500).fillna(False)
        return frame.loc[mask]
    if workload=='aggregate':
        result=frame.groupby('c0',dropna=False,sort=False)['c1'].count().reset_index(name='count')
        return result
    raise ValueError(workload)

def arrow_work(table,kind,workload):
    if workload in ('identity','ipc_sink'):
        return table
    if workload=='filter':
        if kind=='strings':mask=pc.starts_with(table['c0'],'A')
        elif kind=='numeric':mask=pc.less(table['c0'],8)
        else:mask=pc.less(table['c6'],500)
        return table.filter(mask)
    if workload=='aggregate':
        result=table.group_by('c0',use_threads=False).aggregate([('c1','count')])
        return result.rename_columns(['c0','count'])
    raise ValueError(workload)

def output_schema(source,workload):
    if workload=='aggregate':
        return Schema(arrow_schema=pa.schema([source.schema.field('c0'),pa.field('count',pa.int64())]))
    return Schema(arrow_schema=source.schema)

def make_input_manager(schema):
    im=InputManager('benchmark-worker',None)
    port=PortIdentity(0,False)
    channel=ChannelIdentity(ActorVirtualIdentity('upstream'),ActorVirtualIdentity('benchmark-worker'),is_control=False)
    im.add_input_port(port,schema,[],[])
    im.register_input(channel,port)
    return im,channel

def legacy(source,kind,workload,batch_size):
    schema=output_schema(source,workload)
    im,channel=make_input_manager(Schema(arrow_schema=source.schema))
    om=OutputManager('benchmark-worker')
    om.add_output_port(PortIdentity(0,False),schema)
    class MeasuredBatch(BatchOperator):
        BATCH_SIZE=batch_size
        def process_batch(self,batch,port):
            yield pandas_work(batch,kind,workload)
    op=MeasuredBatch()
    pending=[]
    output=[]
    def consume(raw_outputs):
        for raw in raw_outputs:
            for row in all_output_to_tuple(raw):
                if row is None:continue
                row.finalize(schema)
                pending.append(row)
                if len(pending)>=batch_size:
                    output.append(om.tuple_to_frame(pending).frame)
                    pending.clear()
    for row in im.process_data_payload(channel,DataFrame(source)):
        consume(op.process_tuple(row,0))
    consume(op.on_finish(0))
    if pending:output.append(om.tuple_to_frame(pending).frame)
    result=pa.concat_tables(output) if output else pa.Table.from_batches([],schema=schema.as_arrow_schema())
    return result

def direct(source,kind,workload,batch_size,pandas_body):
    schema=output_schema(source,workload).as_arrow_schema()
    outputs=[]
    for rb in source.to_batches(max_chunksize=batch_size):
        table=pa.Table.from_batches([rb])
        if pandas_body:
            frame=table.to_pandas(use_threads=False)
            result=pa.Table.from_pandas(pandas_work(frame,kind,workload),schema=schema,preserve_index=False,nthreads=1,safe=True)
        else:
            result=arrow_work(table,kind,workload).cast(schema,safe=True)
        result.validate(full=True)
        outputs.append(result.replace_schema_metadata(None))
    return pa.concat_tables(outputs) if outputs else pa.Table.from_batches([],schema=schema)

def historical(source,path):
    names=source.column_names
    if path=='current':
        tuples=[Tuple({name:accessor for name in names}) for accessor in ArrowTableTupleProvider(source)]
        incoming=Table(tuples)
        outgoing=list(incoming.as_tuples())
        return pa.Table.from_pydict({name:[t.get_serialized_field(name) for t in outgoing] for name in names},schema=source.schema)
    if path=='arrow_pandas':
        frame=Table(source.to_pandas(use_threads=False))
        return pa.Table.from_pandas(frame,preserve_index=False,schema=source.schema,nthreads=1)
    return pa.Table.from_batches(source.to_batches(),schema=source.schema)

def invoke(source,kind,workload,batch_size,path):
    if kind=='original':result=historical(source,path)
    elif path=='current':result=legacy(source,kind,workload,batch_size)
    else:result=direct(source,kind,workload,batch_size,path=='arrow_pandas')
    if workload=='ipc_sink':
        sink=pa.BufferOutputStream()
        with pa.ipc.new_stream(sink,result.schema) as writer:writer.write_table(result)
        return result,sink.getvalue()
    return result,None

def normalized(table,workload):
    table=table.replace_schema_metadata(None).combine_chunks()
    if workload=='aggregate' and table.num_rows:
        table=table.take(pc.sort_indices(table,sort_keys=[('c0','ascending'),('count','ascending')]))
    return table

def fingerprint(table,workload):
    table=normalized(table,workload)
    # Logical canonicalization: IPC bytes can differ for equal values because of
    # validity-buffer layout or padding. This executes outside the measured window.
    raw=json.dumps({'schema':str(table.schema),'data':table.to_pydict()},ensure_ascii=False,separators=(',',':'),default=lambda v:v.isoformat()).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()

def check_output(table,buf,expected,workload):
    table.validate(full=True)
    assert normalized(table,workload).equals(normalized(expected,workload)), 'output values or schema differ'
    if buf is not None:
        assert pa.ipc.open_stream(buf).read_all().equals(table), 'IPC sink round trip differs'

def fixture_expected(source,kind,workload,batch_size):
    if kind=='original':return source
    schema=output_schema(source,workload).as_arrow_schema()
    outputs=[]
    for rb in source.to_batches(max_chunksize=batch_size):
        frame=pa.Table.from_batches([rb]).to_pandas(use_threads=False)
        out=pandas_work(frame,kind,workload)
        outputs.append(pa.Table.from_pandas(out,schema=schema,preserve_index=False,nthreads=1).replace_schema_metadata(None))
    return pa.concat_tables(outputs) if outputs else pa.Table.from_batches([],schema=schema)

def timing(args):
    source=dataset(args.kind,args.rows)
    expected=fixture_expected(source,args.kind,args.workload,args.batch_size)
    records=[]
    for round_id in range(-args.warmups,args.repeats):
        order=PATHS if round_id%2==0 else list(reversed(PATHS))
        for path in order:
            gc.collect()
            start=time.perf_counter_ns()
            table,buf=invoke(source,args.kind,args.workload,args.batch_size,path)
            elapsed=(time.perf_counter_ns()-start)/1e6
            check_output(table,buf,expected,args.workload)
            if round_id>=0:records.append({'round':round_id,'path':path,'ms':elapsed})
            del table,buf
    return {'kind':args.kind,'workload':args.workload,'rows':args.rows,'batch_size':args.batch_size,'samples':records,'output_rows':expected.num_rows,'fingerprint':fingerprint(expected,args.workload),'source_bytes':source.nbytes}

def memory(args):
    source=dataset(args.kind,args.rows)
    # Pool high water begins after input construction; retaining this pool until exit avoids dangling owners.
    pool=pa.proxy_memory_pool(pa.default_memory_pool())
    pa.set_memory_pool(pool)
    gc.collect()
    baseline=psutil.Process().memory_info().rss
    if args.mode=='python_memory':tracemalloc.start(1)
    print(json.dumps({'state':'ready','baseline_rss':baseline,'source_bytes':source.nbytes}),flush=True)
    if sys.stdin.readline().strip()!='GO':raise RuntimeError('missing GO')
    start=time.perf_counter_ns()
    table,buf=invoke(source,args.kind,args.workload,args.batch_size,args.path)
    elapsed=(time.perf_counter_ns()-start)/1e6
    python_peak=tracemalloc.get_traced_memory()[1] if args.mode=='python_memory' else None
    if args.mode=='python_memory':tracemalloc.stop()
    data={'state':'done','path':args.path,'kind':args.kind,'workload':args.workload,'rows':args.rows,'batch_size':args.batch_size,'baseline_rss':baseline,'end_rss':psutil.Process().memory_info().rss,'arrow_pool_peak':pool.max_memory(),'python_peak':python_peak,'ms_instrumented':elapsed,'output_rows':table.num_rows}
    print(json.dumps(data),flush=True)
    # Parent marks the end of the RSS window before correctness checking allocates extra buffers.
    if sys.stdin.readline().strip()!='CHECK':raise RuntimeError('missing CHECK')
    data['fingerprint']=fingerprint(table,args.workload)
    if buf is not None:assert pa.ipc.open_stream(buf).read_all().equals(table)
    print(json.dumps({'state':'checked','fingerprint':data['fingerprint']}),flush=True)
    del table,buf
    gc.collect()
    pa.set_memory_pool(pa.system_memory_pool())
    # The source is owned by the original allocator, not this proxy pool.
    del pool

def selfcheck():
    checks=0
    for kind in ('strings','numeric','mixed'):
        for n in (0,1,33):
            for workload in ('identity','filter','aggregate','ipc_sink'):
                src=dataset(kind,n)
                expected=fixture_expected(src,kind,workload,16)
                for path in PATHS:
                    result,buf=invoke(src,kind,workload,16,path)
                    check_output(result,buf,expected,workload)
                    checks+=1
    # Hand-computable grouped counts, including a NULL key and NULL counted value.
    src=pa.table({'c0':pa.array([1,1,None,None],type=pa.int64()),'c1':pa.array([2,None,3,4],type=pa.int64())})
    expected=pa.table({'c0':pa.array([1,None],type=pa.int64()),'count':pa.array([1,2],type=pa.int64())})
    for path in PATHS:
        out,_=invoke(src,'numeric','aggregate',16,path)
        check_output(out,None,expected,'aggregate');checks+=1
    print(json.dumps({'checks_passed':checks,'pyarrow':pa.__version__,'pandas':pd.__version__,'python':sys.version}),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--mode',choices=['time','rss','python_memory','selfcheck'],default='time')
    p.add_argument('--kind',default='strings')
    p.add_argument('--workload',default='identity')
    p.add_argument('--rows',type=int,default=10000)
    p.add_argument('--batch-size',type=int,default=4096)
    p.add_argument('--repeats',type=int,default=5)
    p.add_argument('--warmups',type=int,default=1)
    p.add_argument('--path',choices=PATHS,default='current')
    args=p.parse_args()
    if args.mode=='selfcheck':selfcheck()
    elif args.mode=='time':print(json.dumps(timing(args)),flush=True)
    else:memory(args)
