package reactor.core.publisher;

public class InnerStressSubscriber<T> extends StressSubscriber<T> {

	final StressSubscriber<?> parent;

	InnerStressSubscriber(StressSubscriber<?> parent) {
		this.parent = parent;
	}

	@Override
	public void onComplete() {
		super.onComplete();
		this.parent.subscription.request(1);
	}

	@Override
	public void cancel() {
		super.cancel();
	}
}