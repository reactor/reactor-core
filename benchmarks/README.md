## Usage of JMH tasks

Only execute specific benchmark(s) (wildcards are added before and after):
```
../gradlew jmh --include="(BenchmarkPrimary|OtherBench)"
```
If you want to specify the wildcards yourself, you can pass the full regexp:
```
../gradlew jmh --fullInclude=.*MyBenchmark.*
```

Specify extra profilers:
```
../gradlew jmh --profilers="gc,stack"
```

Prominent profilers (for full list call `jmhProfilers` task):
- comp - JitCompilations, tune your iterations
- stack - which methods used most time
- gc - print garbage collection stats
- hs_thr - thread usage

Change report format from JSON to one of [CSV, JSON, NONE, SCSV, TEXT]:
```
./gradlew jmh --format=csv
```

Specify JVM arguments:
```
../gradlew jmh --jvmArgs="-Dtest.cluster=local"
```

Run in verification mode (execute benchmarks with minimum of fork/warmup-/benchmark-iterations):
```
../gradlew jmh --verify=true
```

## Comparing with the baseline
If you wish you run two sets of benchmarks, one for the current change and another one for the "baseline",
there is an additional task `jmhBaseline` that will use the latest release:
```
../gradlew jmh jmhBaseline --include=MyBenchmark
```

## Resources
- http://tutorials.jenkov.com/java-performance/jmh.html (Introduction)
- http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/ (Samples)
