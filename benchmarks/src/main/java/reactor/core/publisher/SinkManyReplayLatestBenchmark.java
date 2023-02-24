package reactor.core.publisher;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class SinkManyReplayLatestBenchmark {


    @Param({"1000", "10000"})
    int rangeSize;

    public static void main(String[] args) throws Exception {
        reactor.core.scrabble.ShakespearePlaysScrabbleParallelOpt
                s = new reactor.core.scrabble.ShakespearePlaysScrabbleParallelOpt();
        s.init();
        System.out.println(s.measureThroughput());
    }

    @Threads(1)
    @Benchmark
    public void measureLatestSingleton() {
        Sinks.Many<Integer> underTest = Sinks.many().replay().latest();
        Flux.range(0, rangeSize)
                .doOnComplete(underTest::tryEmitComplete)
                .subscribe(underTest::tryEmitNext);
        underTest.asFlux().blockLast();
    }

    @Threads(1)
    @Benchmark
    public void measureSizeBoundLimit1() {
        Sinks.Many<Integer> underTest = new SinkManyReplayProcessor<>(new FluxReplay.SizeBoundReplayBuffer<>(1));
        Sinks.Many<Integer> wrapper = new SinkManySerialized<>(underTest, (ContextHolder) underTest);
        Flux.range(0, rangeSize)
                .doOnComplete(wrapper::tryEmitComplete)
                .subscribe(wrapper::tryEmitNext);
        underTest.asFlux().blockLast();
    }
}
