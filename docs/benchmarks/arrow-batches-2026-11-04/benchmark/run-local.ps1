param([string]$ResultsName = ('rerun-' + (Get-Date -Format 'yyyyMMdd-HHmmss')))
$ErrorActionPreference = 'Stop'
$auditRepo = 'C:\Users\carlo\texera\texera-worktrees\audit-pyamber-arrow'
$auditPython = 'C:\Users\carlo\texera\venv312\Scripts\python.exe'
$benchmarkDir = Join-Path $auditRepo 'target\arrow-batch-bench-20261104'
$auditHead = (& git -C $auditRepo rev-parse HEAD).Trim()
if ($auditHead -ne 'ec3a9dd3ca9c361cbd7f339d8bc78669024e84e8') { throw 'Audit checkout is at a different commit; no reset was performed' }
& git -C $auditRepo diff --quiet HEAD -- .
if ($LASTEXITCODE -ne 0) { throw 'Audit checkout has tracked changes; they were left untouched' }
& $auditPython (Join-Path $PSScriptRoot 'setup_arrow_bench.py')
if ($LASTEXITCODE -ne 0) { throw 'Benchmark setup failed' }
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'bench_arrow_batches.py') -Destination (Join-Path $benchmarkDir 'bench_arrow_batches.py')
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'run_arrow_batch_bench.py') -Destination (Join-Path $benchmarkDir 'run_arrow_batch_bench.py')
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'run_arrow_memory_supplement.py') -Destination (Join-Path $benchmarkDir 'run_arrow_memory_supplement.py')
Push-Location -LiteralPath $auditRepo
try {
    & $auditPython (Join-Path $benchmarkDir 'bench_arrow_batches.py') --mode selfcheck
    if ($LASTEXITCODE -ne 0) { throw 'Benchmark correctness checks failed' }
    & $auditPython (Join-Path $benchmarkDir 'run_arrow_batch_bench.py') --results-name $ResultsName --repeats 5 --rss-repeats 2
    if ($LASTEXITCODE -ne 0) { throw 'Benchmark run failed' }
    & $auditPython (Join-Path $benchmarkDir 'run_arrow_memory_supplement.py') --primary-results $ResultsName --results-name ($ResultsName + '-memory')
    if ($LASTEXITCODE -ne 0) { throw 'Supplemental memory run failed' }
} finally {
    Pop-Location
}
