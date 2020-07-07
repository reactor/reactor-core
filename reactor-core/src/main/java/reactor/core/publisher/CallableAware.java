package reactor.core.publisher;

import reactor.util.annotation.Nullable;

interface CallableAware<T> {

	void onNextFromCallable(@Nullable T item);

	void onErrorFromCallable(Throwable e);
}
