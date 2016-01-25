package reactor.core.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

/**
 * Base class for synchronous sources which have fixed size and can
 * emit its items in a pull fashion, thus avoiding the request-accounting
 * overhead in many cases.
 *
 * @param <T> the content value type
 */
public abstract class SynchronousSource<T> implements Queue<T> {
	// -----------------------------------------------------------------------------------
	// The rest of the methods are not applicable
	// -----------------------------------------------------------------------------------
	
	@Override
	public final boolean offer(T e) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}
	
	@Override
	public final int size() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean contains(Object o) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final Iterator<T> iterator() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final Object[] toArray() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final <U> U[] toArray(U[] a) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean remove(Object o) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean addAll(Collection<? extends T> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final boolean add(T e) {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final T remove() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}

	@Override
	public final T element() {
		throw new UnsupportedOperationException("Operators should not use this method!");
	}
}
