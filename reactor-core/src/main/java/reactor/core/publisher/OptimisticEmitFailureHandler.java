package reactor.core.publisher;

import java.time.Duration;

/**
 * @author Animesh Chaturvedi
 * This implementation deals with the transient failure in case of emit from
 * sink because of competing threads. Busy looping is used as an approach instead
 * of {@link Sinks.EmitFailureHandler#FAIL_FAST} with specified duration
 */
public class OptimisticEmitFailureHandler implements Sinks.EmitFailureHandler {

	/**
	 * Duration for busy looping. Be careful in providing the duration as the
	 * construction, emission and handling for the first try will take more than the
	 * duration provided which defeats the purpose. Provide duration > 100ms
	 */
    private final long duration;

	/**
	 * Static factory method to get Emit failure handler which handles the busy looping
	 * for transient failure case.
	 * @param duration Duration specified for busy looping
	 * @return an instance of {@link OptimisticEmitFailureHandler}
	 */
	public static Sinks.EmitFailureHandler busyLooping(Duration duration){
		return new OptimisticEmitFailureHandler(duration);
	}

	private OptimisticEmitFailureHandler(Duration duration) {
		this.duration = System.nanoTime() + duration.toNanos();
	}

	/**
	 * @param signalType the signal that triggered the emission. Can be either {@link SignalType#ON_NEXT}, {@link SignalType#ON_ERROR} or {@link SignalType#ON_COMPLETE}.
	 * @param emitResult the result of the emission (a failure)
	 * @return Based on the emitResult and the duration provided we provide whether the
	 * emission should be retried after specified duration.
	 */
	@Override
	public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
		return emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED) && (System.nanoTime() < this.duration);
	}
}
