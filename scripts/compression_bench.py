import time, json
import gzip, snappy, lz4.frame, zstandard as zstd
from gen_wind_data import gen_record

def bench_compress(data_bytes, alg):
    t0 = time.perf_counter()
    if alg == "none":
        comp = data_bytes
    elif alg == "gzip":
        comp = gzip.compress(data_bytes)
    elif alg == "snappy":
        comp = snappy.compress(data_bytes)
    elif alg == "lz4":
        comp = lz4.frame.compress(data_bytes)
    elif alg == "zstd":
        cctx = zstd.ZstdCompressor(level=3)
        comp = cctx.compress(data_bytes)
    t1 = time.perf_counter()
    return len(comp), (t1 - t0)

def main():
    N = 5000
    buf = []
    for i in range(N):
        rec = gen_record((i%30)+1)
        s = json.dumps(rec).encode('utf-8')
        buf.append(s)
    algs = ["none","snappy","lz4","zstd","gzip"]
    for alg in algs:
        total_before = 0
        total_after = 0
        total_time = 0.0
        for b in buf:
            total_before += len(b)
            after_len, t = bench_compress(b, alg)
            total_after += after_len
            total_time += t
        ratio = total_after / total_before
        print(f"{alg}: ratio={ratio:.3f} total_time={total_time:.3f}s avg_time_per_msg={total_time/N*1000:.3f}ms")

if __name__ == "__main__":
    main()
