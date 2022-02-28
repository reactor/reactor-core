package reactor.core.publisher;

import java.time.Duration;

public class OptimisticEmitFailureHandler implements Sinks.EmitFailureHandler {
    private static long duration;
	public static Sinks.EmitFailureHandler busyLooping(Duration duration){
		OptimisticEmitFailureHandler.duration = System.nanoTime() + duration.getNano();
		return new OptimisticEmitFailureHandler();
	}
	@Override
	public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
		return emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED) && (System.nanoTime() > duration);
	}
}
