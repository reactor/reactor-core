package reactor.core.publisher;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;

class OptimisticEmitFailureHandlerTest {

	@Test
	void shouldRetryOptimistically() {
		Sinks.One<Object> sink = new InternalOneSinkTest.InternalOneSinkAdapter<Object>() {
			final long duration = Duration.ofMillis(1000).toNanos() + System.nanoTime();
			@Override
			public Sinks.EmitResult tryEmitValue(Object value) {
				return System.nanoTime() > duration ? Sinks.EmitResult.OK : Sinks.EmitResult.FAIL_NON_SERIALIZED;
			}

			@Override
			public Sinks.EmitResult tryEmitEmpty() {
				throw new IllegalStateException();
			}

			@Override
			public Sinks.EmitResult tryEmitError(Throwable error) {
				throw new IllegalStateException();
			}
		};
		assertThatNoException().isThrownBy(() -> {
			sink.emitValue("Hello",
					OptimisticEmitFailureHandler.busyLooping(Duration.ofMillis(1000)));
		});
	}

}
