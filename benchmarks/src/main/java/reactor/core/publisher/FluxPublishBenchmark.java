package reactor.core.publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class FluxPublishBenchmark {
	@Param({"0", "10", "1000", "100000"})
	int rangeSize;

	Flux<Integer> source;

	@Setup(Level.Invocation)
	public void setup() {
		source = Flux.range(0, rangeSize)
		             .hide()
		             .publish()
		             .autoConnect(Runtime.getRuntime()
		                                 .availableProcessors());
	}


	@State(Scope.Thread)
	public static class JmhSubscriber<T> extends CountDownLatch implements CoreSubscriber<T> {

		Blackhole blackhole;

		Subscription s;

		public JmhSubscriber() {
			super(1);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			blackhole.consume(t);
		}

		@Override
		public void onError(Throwable t) {
			blackhole.consume(t);
			countDown();
		}

		@Override
		public void onComplete() {
			countDown();
		}
	}

	@SuppressWarnings("unused")
	@Benchmark
	@Threads(Threads.MAX)
	public Object measureThroughput(Blackhole blackhole, JmhSubscriber<Integer> subscriber) throws InterruptedException {
		subscriber.blackhole = blackhole;
		source.subscribe(subscriber);
		subscriber.await();
		return subscriber;
	}
}
