package reactor.util.repeat;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.ContextView;

import java.util.function.Predicate;

public class RepeatSpec extends Repeat {
    static final Logger log = Loggers.getLogger(RepeatSpec.class);
    private final long repeats;

    RepeatSpec(Predicate<? super ContextView> repeatPredicate, long repeats) {
        this.repeats = repeats;
    }

    static Repeat create(Predicate<? super ContextView> predicate, long times) {
        return new RepeatSpec(predicate, times);
    }

    @Override
    public Flux<Long> generateCompanion(Flux<RepeatSignal> other) {
        return Flux.deferContextual(cv ->
                other
                        .contextWrite(cv)
                        .concatMap(retryWhenState -> {
                            RepeatSignal copy = retryWhenState.copy();
                            long iteration = copy.getRepeatsSoFar();
                            if (iteration > repeats) {
                                return Flux.just(-1L);
                            }
                            return Mono.just(iteration);
                        })
                        .onErrorStop()
        );
    }
}