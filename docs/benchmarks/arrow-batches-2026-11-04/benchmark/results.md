# Arrow batch conversion benchmark results

Local measurements on November 4, 2026, using the existing detached audit worktree at ec3a9dd3ca9c361cbd7f339d8bc78669024e84e8. These are measured conversion and UDF-kernel results. The two direct paths are benchmark prototypes; ArrowBatchOperator is not integrated into the engine.

## Main comparison

Each row below processes 10,000 input rows in one batch and filters about half of them. Current uses the real InputManager, BatchOperator, Tuple.finalize, and OutputManager.tuple_to_frame. Arrow plus pandas keeps the same pandas filter but bypasses the row detours. Pure Arrow uses an equivalent Arrow filter. The latter comparison includes a different UDF implementation as well as conversion changes.

| Input | Current ms | Arrow plus pandas ms | Pure Arrow ms | Current / Arrow plus pandas |
| --- | ---: | ---: | ---: | ---: |
| strings | 1,844.101 | 6.588 | 1.756 | 279.9× |
| numeric | 1,533.630 | 1.696 | 0.521 | 904.5× |
| mixed | 1,453.748 | 2.866 | 0.579 | 507.2× |

Peak process RSS added above the loaded-input baseline, in MiB. Values are medians of two fresh child processes per path. Time and memory are measured in separate runs.

| Input | Current extra RSS | Arrow plus pandas extra RSS | Pure Arrow extra RSS | Reduction with same pandas filter |
| --- | ---: | ---: | ---: | ---: |
| strings | 45.39 | 5.32 | 3.00 | 88.3% |
| numeric | 11.95 | 2.31 | 0.85 | 80.7% |
| mixed | 11.07 | 2.14 | 0.60 | 80.6% |

This memory reduction concerns temporary working memory in these conversion cases, not a measured reduction in total workflow memory.

## Reproduction of the original input shape

The recovered September 9 benchmark used ten columns of repeated 64-character strings and 10,000 rows. Its Table-style round trip omitted output Tuple.finalize, BatchOperator's iterrows path, routing, control, transport and storage. It is a different baseline from the BatchOperator cases above.

With the recovered round-trip formulas and this run's single-thread settings, the current Table conversion took 468.387 ms, direct Arrow/pandas/Arrow took 6.736 ms, and Arrow passthrough took 0.056 ms. Current versus direct pandas was 69.5×. The earlier 77× figure is not substituted for these new measurements. The passthrough is a no-op and provides an upper bound on conversion work avoided.

For the same original input shape, fresh-process RSS increments were 13.85 MiB for the current Table round trip, 7.54 MiB for direct pandas and 0.18 MiB for Arrow passthrough.

## Workloads and correctness

Identity measures the conversion path with a no-op UDF. Filter applies equivalent predicates. Aggregate groups and counts within each input batch; it does not implement a distributed or whole-workflow aggregation. IPC sink serializes the converted identity output to an in-memory Arrow stream and reads it back for validation; it is not a disk or network sink.

The datasets have ten columns. Strings use 64-character values with 64 repeating key patterns and NULLs. Numeric uses integer and floating-point columns with NULLs. Mixed contains strings, Unicode, integers, floats, booleans and timestamps, including NULLs. Values and schemas are compared across all three paths. Aggregation outputs are normalized for ordering. Identity and filter preserve row order.

The preflight passed 111 checks covering empty input, one row, final partial batches, NULLs, all four workloads, all three paths and a hand-computable grouped-count fixture. Every timed result is checked outside the measured window. Fresh memory processes compare logical output fingerprints with the timing fixture, and IPC sink outputs are decoded and checked.

## Measurement method

Environment: Intel Core Ultra 9 275HX, 24 cores, Windows 11, 63.4 GiB RAM, Python 3.12.13, PyArrow 23.0.1, pandas 2.2.3 and NumPy 2.1.0. Child processes are pinned to logical CPU 0; Arrow, NumPy and related pools are limited to one thread. The machine has other active work, so these are local measurements rather than isolated-host estimates.

Each timing cell has one warmup per path and five measured samples per path, with path order reversed on alternating rounds. Garbage collection remains enabled and is collected before each sample. Input generation, imports and correctness checks are outside timing. Output construction, row validation on the current path, safe casting and full Arrow validation on direct paths, and the requested UDF work are inside timing. Results are retained until each operation completes, then released between samples.

Both paths materialize and retain their output batches until the operation finishes. This is a conversion/UDF measurement with bounded input batch sizes, not a fully streaming consumer that releases each output batch immediately. The small manager/operator setup used by the current driver is inside the measured operation; a long-lived engine normally amortizes that setup.

RSS is sampled by the parent process with a 1 ms target interval from a ready signal until operation completion. RSS increments can be affected by allocator reuse. Very brief peaks can be missed; this is not an exact allocation count. Absolute RSS and ranges are below. A separate tracemalloc process measures additional tracked allocations for identity. Tracing overhead does not contaminate the timing or RSS runs. Arrow proxy-pool peaks measure allocations visible to that pool after source construction; they do not include all native allocations. These metrics overlap and must not be added together.

Memory measurements cover the first operation after imports and input construction in a fresh process, including lazy initialization on that path. They do not estimate steady-state RSS of a long-lived worker. Traced allocation peaks provide a second view of temporary allocation that RSS sampling alone can miss.

The driver omits MainLoop coroutine switching, backpressure, inspection/retry, real partitioner dispatch and hash routing, Arrow Flight/JVM execution, and backend storage. It uses a single port and common output batching. End-to-end workflow gains remain unmeasured. The direct prototypes do not establish the new API's multi-port, error, binary serialization or control semantics.

The separate mixed-identity attribution profile confirms the row work in the baseline: 10,000 Tuple.as_series calls, 100,000 Arrow field-accessor calls and 10,000 Tuple.finalize calls. Series/DataFrame construction, output iterrows and per-row dispatch also appear among the hot paths. The profile run is excluded from the timing samples; see profile-current-mixed.txt. This helps explain why the BatchOperator ratios differ from the original Table-only experiment.

## Timing matrix

Milliseconds, median of five samples. Input rows are 10,000 in every case; batch size determines the number of UDF calls. Minimum and maximum timings and throughput are in summary.json and all individual samples are in raw.jsonl.

| Input | Workload | Batch rows | Current ms | Arrow plus pandas ms | Pure Arrow ms |
| --- | --- | ---: | ---: | ---: | ---: |
| strings | identity | 1,024 | 1,623.817 | 10.624 | 1.883 |
| strings | identity | 4,096 | 1,813.095 | 9.194 | 1.586 |
| strings | identity | 10,000 | 2,972.688 | 9.018 | 1.728 |
| strings | filter | 1,024 | 1,914.380 | 17.964 | 3.160 |
| strings | filter | 4,096 | 1,718.772 | 9.092 | 1.870 |
| strings | filter | 10,000 | 1,844.101 | 6.588 | 1.756 |
| strings | aggregate | 1,024 | 1,209.740 | 8.726 | 2.744 |
| strings | aggregate | 4,096 | 1,354.443 | 5.052 | 1.946 |
| strings | aggregate | 10,000 | 1,453.775 | 3.321 | 1.756 |
| strings | ipc_sink | 1,024 | 2,071.895 | 14.091 | 4.634 |
| strings | ipc_sink | 4,096 | 1,773.006 | 9.092 | 3.211 |
| strings | ipc_sink | 10,000 | 2,675.289 | 13.213 | 6.820 |
| numeric | identity | 1,024 | 2,482.954 | 6.324 | 0.999 |
| numeric | identity | 4,096 | 1,777.248 | 2.357 | 0.417 |
| numeric | identity | 10,000 | 1,735.641 | 1.188 | 0.239 |
| numeric | filter | 1,024 | 1,959.648 | 8.492 | 1.896 |
| numeric | filter | 4,096 | 1,718.716 | 5.406 | 0.727 |
| numeric | filter | 10,000 | 1,533.630 | 1.696 | 0.521 |
| numeric | aggregate | 1,024 | 1,322.103 | 7.852 | 1.134 |
| numeric | aggregate | 4,096 | 1,141.706 | 2.816 | 0.869 |
| numeric | aggregate | 10,000 | 1,164.983 | 1.384 | 0.431 |
| numeric | ipc_sink | 1,024 | 1,615.897 | 4.654 | 1.004 |
| numeric | ipc_sink | 4,096 | 1,566.636 | 2.143 | 0.562 |
| numeric | ipc_sink | 10,000 | 1,583.338 | 1.261 | 0.429 |
| mixed | identity | 1,024 | 1,664.732 | 6.630 | 0.893 |
| mixed | identity | 4,096 | 1,704.261 | 3.411 | 0.554 |
| mixed | identity | 10,000 | 2,048.795 | 2.948 | 0.462 |
| mixed | filter | 1,024 | 1,352.439 | 8.225 | 1.465 |
| mixed | filter | 4,096 | 1,374.786 | 3.856 | 0.813 |
| mixed | filter | 10,000 | 1,453.748 | 2.866 | 0.579 |
| mixed | aggregate | 1,024 | 1,051.451 | 6.649 | 2.075 |
| mixed | aggregate | 4,096 | 1,131.624 | 3.445 | 1.620 |
| mixed | aggregate | 10,000 | 1,162.973 | 2.040 | 1.421 |
| mixed | ipc_sink | 1,024 | 1,709.665 | 7.897 | 1.445 |
| mixed | ipc_sink | 4,096 | 2,595.553 | 4.077 | 1.032 |
| mixed | ipc_sink | 10,000 | 1,797.904 | 3.710 | 0.829 |

## Process memory measurements

MiB, median of two independent RSS runs. The range is the minimum and maximum RSS increment.

| Input | Workload | Path | Baseline RSS | Peak RSS | Increment | Increment range |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| strings | identity | current | 145.61 | 163.36 | 17.75 | 17.45–18.06 |
| strings | identity | arrow_pandas | 145.68 | 153.87 | 8.19 | 8.12–8.25 |
| strings | identity | pure_arrow | 145.76 | 145.94 | 0.18 | 0.18–0.18 |
| strings | filter | current | 145.83 | 191.22 | 45.39 | 45.29–45.49 |
| strings | filter | arrow_pandas | 145.87 | 151.19 | 5.32 | 5.28–5.36 |
| strings | filter | pure_arrow | 145.77 | 148.76 | 3.00 | 3.00–3.00 |
| strings | aggregate | current | 145.77 | 157.54 | 11.77 | 11.04–12.51 |
| strings | aggregate | arrow_pandas | 145.83 | 147.64 | 1.81 | 1.77–1.86 |
| strings | aggregate | pure_arrow | 145.62 | 146.38 | 0.75 | 0.75–0.75 |
| strings | ipc_sink | current | 145.65 | 177.72 | 32.07 | 31.98–32.16 |
| strings | ipc_sink | arrow_pandas | 145.90 | 168.46 | 22.55 | 22.48–22.62 |
| strings | ipc_sink | pure_arrow | 145.45 | 160.00 | 14.56 | 14.56–14.56 |
| numeric | identity | current | 137.00 | 151.24 | 14.24 | 14.02–14.46 |
| numeric | identity | arrow_pandas | 137.07 | 138.97 | 1.90 | 1.87–1.93 |
| numeric | identity | pure_arrow | 137.07 | 137.22 | 0.15 | 0.15–0.15 |
| numeric | filter | current | 137.10 | 149.05 | 11.95 | 11.42–12.48 |
| numeric | filter | arrow_pandas | 137.19 | 139.50 | 2.31 | 2.29–2.32 |
| numeric | filter | pure_arrow | 136.98 | 137.83 | 0.85 | 0.85–0.85 |
| numeric | aggregate | current | 137.09 | 148.83 | 11.73 | 11.73–11.73 |
| numeric | aggregate | arrow_pandas | 137.12 | 139.05 | 1.93 | 1.93–1.93 |
| numeric | aggregate | pure_arrow | 136.97 | 137.77 | 0.80 | 0.80–0.80 |
| numeric | ipc_sink | current | 136.99 | 152.72 | 15.73 | 15.59–15.88 |
| numeric | ipc_sink | arrow_pandas | 137.05 | 141.16 | 4.11 | 4.07–4.15 |
| numeric | ipc_sink | pure_arrow | 137.00 | 139.31 | 2.31 | 2.31–2.31 |
| mixed | identity | current | 140.46 | 152.10 | 11.64 | 11.56–11.71 |
| mixed | identity | arrow_pandas | 140.59 | 143.34 | 2.76 | 2.64–2.88 |
| mixed | identity | pure_arrow | 140.55 | 140.70 | 0.15 | 0.15–0.15 |
| mixed | filter | current | 140.54 | 151.62 | 11.07 | 10.66–11.48 |
| mixed | filter | arrow_pandas | 140.58 | 142.73 | 2.14 | 2.12–2.16 |
| mixed | filter | pure_arrow | 140.67 | 141.27 | 0.60 | 0.60–0.60 |
| mixed | aggregate | current | 140.40 | 150.79 | 10.38 | 9.58–11.18 |
| mixed | aggregate | arrow_pandas | 140.75 | 141.86 | 1.10 | 1.09–1.11 |
| mixed | aggregate | pure_arrow | 140.45 | 141.14 | 0.70 | 0.70–0.70 |
| mixed | ipc_sink | current | 140.33 | 155.20 | 14.87 | 13.95–15.79 |
| mixed | ipc_sink | arrow_pandas | 140.50 | 146.32 | 5.83 | 5.82–5.83 |
| mixed | ipc_sink | pure_arrow | 140.53 | 143.87 | 3.34 | 3.34–3.34 |

## Additional tracked allocations

One separate traced identity run per path, 10,000 rows in one batch, MiB. Tracemalloc can include extension allocations registered with Python; it is not a complete process-memory measurement. Pure Arrow identity can share input buffers, so a zero Arrow-pool peak is expected for the no-op case and does not imply zero memory use.

| Input | Path | Peak tracked allocations | Arrow pool peak |
| --- | --- | ---: | ---: |
| original | current | 28.25 | 6.88 |
| original | arrow_pandas | 0.81 | 6.88 |
| original | pure_arrow | 0.01 | 0.00 |
| strings | current | 47.80 | 6.77 |
| strings | arrow_pandas | 0.87 | 6.77 |
| strings | pure_arrow | 0.01 | 0.00 |
| numeric | current | 37.96 | 0.77 |
| numeric | arrow_pandas | 0.04 | 0.92 |
| numeric | pure_arrow | 0.01 | 0.00 |
| mixed | current | 41.60 | 1.34 |
| mixed | arrow_pandas | 0.36 | 1.60 |
| mixed | pure_arrow | 0.01 | 0.00 |

## Memory at smaller batch sizes

Identity conversion, 10,000 input rows, median extra RSS from two fresh processes per path, MiB. This checks how the memory result changes when the same input is processed in bounded batches.

| Input | Batch rows | Current extra RSS | Arrow plus pandas extra RSS | Pure Arrow extra RSS |
| --- | ---: | ---: | ---: | ---: |
| strings | 1,024 | 15.47 | 10.22 | 0.19 |
| strings | 4,096 | 17.45 | 7.95 | 0.19 |
| strings | 10,000 | 17.75 | 8.19 | 0.18 |
| numeric | 1,024 | 6.80 | 2.02 | 0.17 |
| numeric | 4,096 | 13.61 | 1.94 | 0.17 |
| numeric | 10,000 | 14.24 | 1.90 | 0.15 |
| mixed | 1,024 | 6.60 | 1.70 | 0.16 |
| mixed | 4,096 | 15.71 | 1.77 | 0.16 |
| mixed | 10,000 | 11.64 | 2.76 | 0.15 |

## Reproduction and repository changes

Run the setup script to generate Python protobuf files, then the selfcheck and controller using the existing Python 3.12 environment. Use a new results directory name for another run; the controller refuses to overwrite existing samples. The scripts retain the original local worktree paths as defaults.

Commands used:

    python setup_arrow_bench.py
    C:\Users\carlo\texera\venv312\Scripts\python.exe bench_arrow_batches.py --mode selfcheck
    C:\Users\carlo\texera\venv312\Scripts\python.exe run_arrow_batch_bench.py --results-name full-results --repeats 5 --rss-repeats 2

The controller runs every child with the audit worktree as its working directory. Generated modules, scripts and raw run results are under target/arrow-batch-bench-20261104 and are ignored by Git. Engine source hashes were checked before and after the full run and match. No tracked source was changed, and nothing was committed, pushed or posted.
