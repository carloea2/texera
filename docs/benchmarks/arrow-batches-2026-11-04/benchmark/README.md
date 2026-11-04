# Arrow batch benchmark package

Read results.md for the measured time and memory comparisons, methodology, and limits. The benchmark runs against the existing audit worktree at ec3a9dd3ca9c361cbd7f339d8bc78669024e84e8. Its direct Arrow paths are standalone benchmark prototypes, not an implemented engine API.

The primary run contains 37 timing cells, with three paths and five measured samples per path, plus one warmup per path. RSS was measured separately in fresh processes, twice per path and case. The supplemental run covers the original Table-conversion input and identity conversion with smaller batches. Traced allocations are measured separately so tracing overhead does not affect the time or RSS comparisons.

Files:

- results.md: readable results and scope.
- summary.json: timing medians, ranges, throughput, RSS summaries and allocation measurements.
- raw.jsonl and supplemental-memory.jsonl: individual measured samples.
- environment.json: machine, affinity, source hashes and timing-run configuration.
- repository-proof.json: final audit checkout status and source-integrity check.
- preflight.json: correctness checks performed before the measured run.
- profile-current-mixed.txt: a separate attribution profile, excluded from timing samples.
- bench_arrow_batches.py: dataset generation, real-helper baseline, direct prototypes and checks.
- run_arrow_batch_bench.py and run_arrow_memory_supplement.py: measurement controllers.
- setup_arrow_bench.py: protobuf generation using the cached protoc compiler and existing Python environment.
- run-local.ps1: local reproduction entrypoint; preserves existing results and refuses a changed audit baseline.
- manifest.json: SHA-256 hashes for the packaged files.

To rerun locally, extract the package and run run-local.ps1 with a new ResultsName. It uses the existing worktree and Python environment; the hardcoded local paths are visible at the top of the scripts. It writes generated code and measurements under the ignored target directory and does not reset, commit, push, or publish anything.

Input generation, imports, and output comparison are excluded from timing. The sink case uses an in-memory Arrow IPC stream. Routing/hash dispatch, MainLoop switching, control, backpressure, real storage and Arrow Flight/JVM execution are not benchmarked. Input and output tables are materialized in memory, so these measurements do not establish full-workflow performance or steady-state streaming memory.
