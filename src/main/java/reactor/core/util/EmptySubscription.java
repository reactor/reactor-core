package reactor.core.util;

import java.util.Collection;
import java.util.Iterator;

import org.reactivestreams.Subscriber;
import reactor.core.flow.Fuseable;
import reactor.core.state.Introspectable;

/**
 * A singleton enumeration that represents a no-op Subscription instance that can be freely given out to clients.
 */
public enum EmptySubscription implements Fuseable.QueueSubscription<Object>, Introspectable {
	INSTANCE;

	@Override
	public void request(long n) {
		// deliberately no op
	}

	@Override
	public void cancel() {
		// deliberately no op
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onError with the
	 * supplied error.
	 *
	 * @param s
	 * @param e
	 */
	public static void error(Subscriber<?> s, Throwable e) {
		s.onSubscribe(INSTANCE);
		s.onError(e);
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onComplete.
	 *
	 * @param s
	 */
	public static void complete(Subscriber<?> s) {
		s.onSubscribe(INSTANCE);
		s.onComplete();
	}

	@Override
	public int getMode() {
		return TRACE_ONLY;
	}

	@Override
	public String getName() {
		return INSTANCE.name();
	}

	@Override
	public boolean add(Object e) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public boolean offer(Object e) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public Object remove() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public Object poll() {
		return null;
	}

	@Override
	public Object element() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public Object peek() {
		return null;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public Iterator<Object> iterator() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public boolean addAll(Collection<? extends Object> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public void clear() {
		// deliberately no op
	}

	@Override
	public int requestFusion(int requestedMode) {
		return Fuseable.NONE; // can't enable fusion due to complete/error possibility
	}

	@Override
	public void drop() {
		// deliberately no op
	}

}
