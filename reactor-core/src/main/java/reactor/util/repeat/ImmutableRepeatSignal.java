package reactor.util.repeat;

import reactor.util.context.ContextView;

public class ImmutableRepeatSignal implements Repeat.RepeatSignal {
    final Throwable failure;
    final ContextView retryContext;
    final long repeatsSoFar;

    public ImmutableRepeatSignal(Throwable failure, ContextView retryContext, long repeatsSoFar) {
        this.failure = failure;
        this.retryContext = retryContext;
        this.repeatsSoFar = repeatsSoFar;
    }

    @Override
    public Throwable failure() {
        return failure;
    }

    @Override
    public long getRepeatsSoFar() {
        return repeatsSoFar;
    }
}
