package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLL_Result;
import reactor.core.util.FastLogger;
import reactor.test.scheduler.VirtualTimeScheduler;

public class FluxBufferTimeoutStressTest {

	@JCStressTest
	@Outcome(id = "1, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "2, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndTimeout {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>();

		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), null);

		final StressSubscription<Long> subscription = new StressSubscription<>(bufferTimeoutSubscriber);

		{
			bufferTimeoutSubscriber.onSubscribe(subscription);
		}

		@Actor
		public void next() {
			bufferTimeoutSubscriber.onNext(0L);
			bufferTimeoutSubscriber.onNext(1L);
			bufferTimeoutSubscriber.onComplete();
		}

		@Actor
		public void timeout() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscriber.onCompleteCalls.get();
			result.r3 = subscription.requestsCount.get();

			if (subscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("unexpected completion " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("subscriber concurrent onNext");
			}
		}
	}

	@JCStressTest
	@Outcome(id = "3, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "4, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndMoreTimeouts {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>();

		final FastLogger fastLogger = new FastLogger(getClass().getName());
		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), fastLogger);

		final StressSubscription<Long> subscription = new StressSubscription<>(bufferTimeoutSubscriber);

		{
			bufferTimeoutSubscriber.onSubscribe(subscription);
		}

		@Actor
		public void next() {
			bufferTimeoutSubscriber.onNext(0L);
			bufferTimeoutSubscriber.onNext(1L);
			bufferTimeoutSubscriber.onNext(2L);
			bufferTimeoutSubscriber.onNext(3L);
			bufferTimeoutSubscriber.onNext(4L);

			bufferTimeoutSubscriber.onComplete();
		}

		@Actor
		public void timeout() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscriber.onCompleteCalls.get();
			result.r3 = subscription.requestsCount.get();

			if (subscriber.onCompleteCalls.get() != 1) {
				throw new IllegalStateException("unexpected completion " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("subscriber concurrent onNext");
			}
			if (subscriber.receivedValues.stream().anyMatch(List::isEmpty)) {
				throw new IllegalStateException("received an empty buffer: " + subscriber.receivedValues + "; result=" + result + "\n" + fastLogger);
			}
		}
	}

	@JCStressTest
	@Outcome(id = "5, 1, 2", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 1, 3", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 1, 4", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 1, 5", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 0, 2", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 0, 3", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 0, 4", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 0, 5", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndMoreTimeoutsPossiblyIncomplete {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>(1);

		final FastLogger fastLogger = new FastLogger(getClass().getName());
		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), fastLogger);

		Sinks.Many<Long> proxy = Sinks.unsafe().many().unicast().onBackpressureBuffer();
		final AtomicLong requested = new AtomicLong();
		{
			proxy.asFlux()
			     .doOnRequest(r -> requested.incrementAndGet())
			     .subscribe(bufferTimeoutSubscriber);
		}

		@Actor
		public void next() {
			proxy.tryEmitNext(0L);
			proxy.tryEmitNext(1L);
			proxy.tryEmitNext(2L);
			proxy.tryEmitNext(3L);
			proxy.tryEmitNext(4L);

			proxy.tryEmitNext(5L);
			proxy.tryEmitNext(6L);
			proxy.tryEmitNext(7L);
			proxy.tryEmitNext(8L);
			proxy.tryEmitNext(9L);
			proxy.tryEmitComplete();
		}

		@Actor
		public void timeout() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Actor
		public void request() {
			subscriber.request(1);
			subscriber.request(1);
			subscriber.request(1);
			subscriber.request(1);
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscriber.onCompleteCalls.get();
			result.r3 = requested.get();

			if (subscriber.onCompleteCalls.get() == 0) {
				if (subscriber.receivedValues.stream()
				                             .noneMatch(buf -> buf.size() == 1)) {
					throw new IllegalStateException("incomplete but received all two " +
							"element buffers. received: " + subscriber.receivedValues + "; result=" + result + "\n" + fastLogger);
				}
			}

			if (subscriber.onNextCalls.get() < 5 && subscriber.onCompleteCalls.get() == 0) {
				throw new IllegalStateException("incomplete. received: " + subscriber.receivedValues + "; requested=" + requested.get() + "; result=" + result + "\n" + fastLogger);
			}

			if (subscriber.onCompleteCalls.get() > 1) {
				throw new IllegalStateException("unexpected completion " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				throw new IllegalStateException("subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				throw new IllegalStateException("subscriber concurrent onNext");
			}
			if (subscriber.receivedValues.stream().anyMatch(List::isEmpty)) {
				throw new IllegalStateException("received an empty buffer: " + subscriber.receivedValues + "; result=" + result + "\n" + fastLogger);
			}
		}
	}

	private static Supplier<List<Long>> bufferSupplier() {
		return ArrayList::new;
	}
}
