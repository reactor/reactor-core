package reactor.core;

import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Schedulers;

public class TestHook1 {

    private static final Logger logger = LoggerFactory.getLogger(TestHook1.class);

    static {
        try {
            Hooks.onEachOperator("testHook", Operators.lift(new Lifter<>()));
        } catch (Throwable ignored) {
        }
    }


    public static class Lifter<T>
            implements
            BiFunction<Scannable, CoreSubscriber<? super T>, CoreSubscriber<? super T>> {

        /**
         * Holds reference to strategy to prevent it from being collected.
         */

        @Override
        public CoreSubscriber<? super T> apply(Scannable publisher, CoreSubscriber<? super T> sub) {
            return sub;
        }
    }
    public static void main(String[] args) throws InterruptedException {

        Flux.fromStream(() -> {
            logger.info("Operation fromStream enter call1: ");
            Mono.just("test1").block();
            return  Stream.of(
                    "scene 1",
                    "scene 2",
                    "scene 3",
                    "scene 4",
                    "scene 5"
            );
        }).subscribeOn(Schedulers.boundedElastic()).publishOn(Schedulers.boundedElastic()).subscribeOn(Schedulers.single()).subscribe();

        Thread.sleep(3000);
    }
}