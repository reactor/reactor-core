package reactor.core.publisher;

import java.io.IOException;
import java.util.function.Function;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;
import reactor.core.publisher.Mono;

@State(Scope.Benchmark)
@Fork(1)
public class FusionProof {

//    @Param({"1", "31", "1024", "10000"})
//    public static int elementCount;

    @Benchmark()
    @BenchmarkMode(Mode.Throughput)
    public void monoFussedOptimizationProof() {
        Mono.just(1)
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .block();
    }

    @Benchmark()
    @BenchmarkMode(Mode.Throughput)
    public void monoNonFusedChain() {
        Mono<Integer> source = Mono.just(1);
        MonoMapFuseable<Integer, Integer> map1 = new MonoMapFuseable<>(source, Function.identity());
        MonoMapFuseable<Integer, Integer> map2 = new MonoMapFuseable<>(map1, Function.identity());
        MonoMapFuseable<Integer, Integer> map3 = new MonoMapFuseable<>(map2, Function.identity());
        MonoMapFuseable<Integer, Integer> map4 = new MonoMapFuseable<>(map3, Function.identity());
        MonoMapFuseable<Integer, Integer> map5 = new MonoMapFuseable<>(map4, Function.identity());
        MonoMapFuseable<Integer, Integer> map6 = new MonoMapFuseable<>(map5, Function.identity());
        MonoMapFuseable<Integer, Integer> map7 = new MonoMapFuseable<>(map6, Function.identity());
        MonoMapFuseable<Integer, Integer> map8 = new MonoMapFuseable<>(map7, Function.identity());
        MonoMapFuseable<Integer, Integer> map9 = new MonoMapFuseable<>(map8, Function.identity());
        MonoMapFuseable<Integer, Integer> map10 = new MonoMapFuseable<>(map9, Function.identity());
        map10.block();
    }


    @Benchmark()
    @BenchmarkMode(Mode.Throughput)
    public void flux100000FussedOptimizationProof() {
        Flux.range(1, 100000)
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .map(Function.identity())
            .blockLast();
    }

    @Benchmark()
    @BenchmarkMode(Mode.Throughput)
    public void flux100000NonFusedChain() {
        Flux<Integer> source = Flux.range(1, 100000);
        FluxMapFuseable<Integer, Integer> map1 = new FluxMapFuseable<>(source, Function.identity());
        FluxMapFuseable<Integer, Integer> map2 = new FluxMapFuseable<>(map1, Function.identity());
        FluxMapFuseable<Integer, Integer> map3 = new FluxMapFuseable<>(map2, Function.identity());
        FluxMapFuseable<Integer, Integer> map4 = new FluxMapFuseable<>(map3, Function.identity());
        FluxMapFuseable<Integer, Integer> map5 = new FluxMapFuseable<>(map4, Function.identity());
        FluxMapFuseable<Integer, Integer> map6 = new FluxMapFuseable<>(map5, Function.identity());
        FluxMapFuseable<Integer, Integer> map7 = new FluxMapFuseable<>(map6, Function.identity());
        FluxMapFuseable<Integer, Integer> map8 = new FluxMapFuseable<>(map7, Function.identity());
        FluxMapFuseable<Integer, Integer> map9 = new FluxMapFuseable<>(map8, Function.identity());
        FluxMapFuseable<Integer, Integer> map10 = new FluxMapFuseable<>(map9, Function.identity());
        map10.blockLast();
    }
}
