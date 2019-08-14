package reactor.core.publisher;

import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import reactor.core.Disposable;

class ChangedPredicate<T, K> implements Predicate<T>, Disposable {

	private Function<? super T, ? extends K>  keySelector;
	private BiPredicate<? super K, ? super K> keyComparator;
	private K                                 lastKey;

	ChangedPredicate(Function<? super T, ? extends K> keySelector,
			BiPredicate<? super K, ? super K> keyComparator) {
		this.keySelector = keySelector;
		this.keyComparator = keyComparator;
	}

	@Override
	public void dispose() {
		lastKey = null;
	}

	@Override
	public boolean test(T t) {
		K k = keySelector.apply(t);

		if (null == lastKey) {
			lastKey = k;
			return false;
		}

		boolean match;
		match = keyComparator.test(lastKey, k);
		lastKey = k;

		return !match;
	}
}
