import json
import os
import subprocess
from pathlib import Path

REPO=Path(r'C:\Users\carlo\texera\texera-worktrees\audit-pyamber-arrow')
BENCH=REPO/'target/arrow-batch-bench-20261104'
PYTHON=Path(r'C:\Users\carlo\texera\venv312\Scripts\python.exe')
PROTOC=Path(r'C:\Users\carlo\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\protobuf\protoc\3.19.4\protoc-3.19.4-windows-x86_64.exe')
PLUGIN=PYTHON.parent/'protoc-gen-python_betterproto.exe'
INCLUDES=[REPO/'amber/src/main/protobuf',REPO/'common/workflow-core/src/main/protobuf',Path(r'C:\Users\carlo\protoinc_google')]
BENCH.mkdir(parents=True,exist_ok=True)
GENERATED=BENCH/'generated'
PROTOOUT=GENERATED/'proto'
PROTOOUT.mkdir(parents=True,exist_ok=True)
files=[p for root in INCLUDES[:2] for p in root.rglob('*.proto')]
cmd=[str(PROTOC),f'--plugin=protoc-gen-python_betterproto={PLUGIN}',f'--python_betterproto_out={PROTOOUT}']+[f'-I={p}' for p in INCLUDES]+[str(p) for p in files]
result=subprocess.run(cmd,cwd=REPO,text=True,capture_output=True)
(BENCH/'proto-setup.log').write_text(result.stdout+result.stderr,encoding='utf-8')
print('Proto generation exit',result.returncode,'from',len(files),'source files')
if result.returncode:
    print((result.stdout+result.stderr)[-3000:])
    raise SystemExit(result.returncode)
env=os.environ.copy()
env['PYTHONPATH']=os.pathsep.join([str(GENERATED),str(REPO/'amber/src/main/python')])
check=subprocess.run([str(PYTHON),'-c','from core.models import Schema, Tuple, BatchOperator; from core.architecture.packaging.input_manager import InputManager; from core.architecture.packaging.output_manager import OutputManager; print("Real engine imports available")'],cwd=REPO,env=env,text=True,capture_output=True)
print(check.stdout,check.stderr)
(BENCH/'setup.json').write_text(json.dumps({'repository':str(REPO),'python':str(PYTHON),'generated':str(GENERATED),'proto_files':len(files),'protoc':str(PROTOC),'import_exit':check.returncode},indent=2),encoding='utf-8')
raise SystemExit(check.returncode)
