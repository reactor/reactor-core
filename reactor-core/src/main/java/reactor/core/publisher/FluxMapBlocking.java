package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.function.Function;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;

public class FluxMapBlocking<IN, OUT> extends InternalFluxOperator<IN, OUT> {

	final Function<? super IN, ? extends OUT> mapper;
	final Scheduler                        scheduler;
	final int                              concurrency;

	/**
	 * Build a {@link InternalFluxOperator} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	protected FluxMapBlocking(Flux<? extends IN> source,
			Function<? super IN, ? extends OUT> mapper,
			Scheduler scheduler,
			int concurrency) {
		super(source);
		this.mapper = mapper;
		this.scheduler = scheduler;
		this.concurrency = concurrency;
	}

	@Override
	public CoreSubscriber<? super IN> subscribeOrReturn(CoreSubscriber<? super OUT> actual) throws Throwable {
		return null;
	}


	static class MapBlocking<T, R> implements InnerOperator<T, R> {

		final CoreSubscriber<? super R>        actual;
		final Function<? super T, ? extends R> mapper;
		final Scheduler                        scheduler;
		final Queue<R>                         queue;

		boolean done;

		Subscription s;
		int index;

		MapBlocking(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends R> mapper,
				Scheduler scheduler, Queue<R> queue) {
			this.actual = actual;
			this.mapper = mapper;
			this.scheduler = scheduler;
			this.queue = queue;
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return this.actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
			}
		}

		@Override
		public void onNext(T t) {
			scheduler.schedule(() -> {
				R result;
				try {
				 result = Objects.requireNonNull(mapper.apply(t));
				} catch (Throwable ex) {

					return;
				}

				queue.offer(result);
			});
		}

		@Override
		public void onError(Throwable t) {

		}

		@Override
		public void onComplete() {

		}

		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}
	}
}
