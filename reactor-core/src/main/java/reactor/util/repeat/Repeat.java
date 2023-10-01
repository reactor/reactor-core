package reactor.util.repeat;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.function.Predicate;

public abstract class Repeat {

    public final ContextView repeatContext;

    public Repeat() {
        this(Context.empty());
    }

    protected Repeat(ContextView repeatContext) {
        this.repeatContext = repeatContext;
    }

    /**
     * Repeat function that repeats only if the predicate returns true.
     * @param predicate Predicate that determines if next repeat is performed
     * @return Repeat function with predicate
     */
    public static Repeat onlyIf(Predicate<? super ContextView> predicate) {
        return RepeatSpec.create(predicate, Long.MAX_VALUE);
    }

    /**
     * Repeat function that repeats n times.
     * @param n number of repeats
     * @return Repeat function for n repeats
     */
    public static Repeat times(long n) {
        if (n < 0)
            throw new IllegalArgumentException("n should be >= 0");
        return RepeatSpec.create(context -> true, n);
    }

    /**
     * Repeat function that repeats n times, only if the predicate returns true.
     * @param predicate Predicate that determines if next repeat is performed
     * @param n number of repeats
     * @return Repeat function with predicate and n repeats
     */
    static Repeat create(Predicate<? super ContextView> predicate, long n) {
        return RepeatSpec.create(predicate, n);
    }

    public abstract Publisher<?> generateCompanion(Flux<RepeatSignal> other);

    public interface RepeatSignal {
        Throwable failure();
        default RepeatSignal copy() {
            return new ImmutableRepeatSignal(failure(), retryContextView(), getRepeatsSoFar());
        }
        default ContextView retryContextView() {
            return Context.empty();
        }

        long getRepeatsSoFar();
    }

    public ContextView getRepeatContext() {
        return repeatContext;
    }

}
