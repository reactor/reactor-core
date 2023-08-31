package reactor.core.publisher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

public class ThreadSwitchingParallelFlux<T> extends ParallelFlux<T> implements
                                                                    Subscription, Runnable {

	private final T item;
	private final ExecutorService executorService;
	AtomicBoolean done = new AtomicBoolean();
	CoreSubscriber<? super T>[] actual;

	public ThreadSwitchingParallelFlux(T item, ExecutorService executorService) {
		this.item = item;
		this.executorService = executorService;
	}

	@Override
	public int parallelism() {
		return 1;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}

		this.actual = subscribers;
		executorService.submit(this);
	}

	@Override
	public void run() {
		actual[0].onSubscribe(this);
	}

	private void deliver() {
		if (done.compareAndSet(false, true)) {
			this.actual[0].onNext(this.item);
			this.actual[0].onComplete();
		}
	}

	@Override
	public void request(long n) {
		if (Operators.validate(n)) {
			if (!done.get()) {
				this.executorService.submit(this::deliver);
			}
		}
	}

	@Override
	public void cancel() {
		done.set(true);
	}
}
