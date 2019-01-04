package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingTest {

	@Rule
	public TestName testName = new TestName();

	@After
	public void tearDown() {
		new HashSet<>(Hooks.getTracers().keySet()).forEach(Hooks::removeTracer);
	}

	@Test
	public void shouldCreateSpan() {
		TestTracer tracer = new TestTracer();
		Hooks.addTracer(testName.getMethodName(), tracer);

		Flux
				.just(1)
				.blockLast();

		assertThat(tracer.getSpansRecorded()).hasSize(1);
	}

	@Test
	public void shouldPropogateSpanToThreads() {
		TestTracer tracer = new TestTracer();
		Hooks.addTracer(testName.getMethodName(), tracer);

		Flux
				.just(1)
				.flatMap(it -> {
					return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
					           .doOnNext(__ -> {
						           assertThat(tracer.getCurrentSpan())
								           .hasValue(tracer.getSpansRecorded().get(0));
					           });
				})
				.blockLast();

		assertThat(tracer.getSpansRecorded()).hasSize(1);
	}

	private static class TestTracer implements Tracer {

		final List<Span>            spansRecorded = new ArrayList<>();
		final AtomicReference<Span> currentSpan   = new AtomicReference<>();

		boolean shouldTrace = true;

		List<Span> getSpansRecorded() {
			return spansRecorded;
		}

		AtomicReference<Span> getCurrentSpan() {
			return currentSpan;
		}

		@Override
		public boolean shouldTrace() {
			return shouldTrace;
		}

		@Override
		public void onSpanCreated(Span span) {
			spansRecorded.add(span);
		}

		@Override
		public Disposable onScopePassing(Span span) {
			currentSpan.set(span);
			return () -> currentSpan.set(null);
		}
	}
}