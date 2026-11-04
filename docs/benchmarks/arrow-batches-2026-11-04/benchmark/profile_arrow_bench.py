import cProfile
import io
import pstats
from pathlib import Path
import bench_arrow_batches as bench

out=Path(__file__).resolve().parent/'full-results'
source=bench.dataset('mixed',10000)
profiler=cProfile.Profile()
profiler.enable()
result,_=bench.invoke(source,'mixed','identity',10000,'current')
profiler.disable()
assert bench.normalized(result,'identity').equals(bench.normalized(source,'identity'))
stream=io.StringIO()
stream.write('Attribution profile only; this run is not part of the timing samples.\n')
pstats.Stats(profiler,stream=stream).strip_dirs().sort_stats('cumtime').print_stats(30)
(out/'profile-current-mixed.txt').write_text(stream.getvalue(),encoding='utf-8')
print(stream.getvalue())
