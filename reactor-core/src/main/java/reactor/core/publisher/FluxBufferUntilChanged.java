package reactor.core.publisher;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class FluxBufferUntilChanged<T, K, C extends Collection<? super T>>
		extends InternalFluxOperator<T, C> {

	final Function<? super T, K>            keyExtractor;
	final BiPredicate<? super K, ? super K> keyComparator;
	final Supplier<C>                       bufferSupplier;

	FluxBufferUntilChanged(Flux<? extends T> source,
			Function<? super T, K> keyExtractor,
			BiPredicate<? super K, ? super K> keyComparator,
			Supplier<C> bufferSupplier) {
		super(source);
		this.keyExtractor = keyExtractor;
		this.keyComparator = keyComparator;
		this.bufferSupplier = bufferSupplier;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super C> actual) {
		C initialBuffer;

		try {
			initialBuffer = Objects.requireNonNull(bufferSupplier.get(),
					"The bufferSupplier returned a null initial buffer");
		}
		catch (NullPointerException e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return null;
		}

		return new BufferUntilChangedSubscriber<>(actual, keyExtractor,
				keyComparator,
				initialBuffer,
				bufferSupplier);
	}

	static final class BufferUntilChangedSubscriber<T, K, C extends Collection<? super T>>
			extends AbstractQueue<C>
			implements ConditionalSubscriber<T>, InnerOperator<T, C>, BooleanSupplier {

		final CoreSubscriber<? super C>         actual;
		final Supplier<C>                       bufferSupplier;
		final Function<? super T, K>            keyExtractor;
		final BiPredicate<? super K, ? super K> keyComparator;

		volatile Subscription s;
		boolean done;
		C       buffer;
		volatile boolean fastpath;
		volatile long    requested;

		static final AtomicLongFieldUpdater<BufferUntilChangedSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(BufferUntilChangedSubscriber.class,
						"requested");

		static final AtomicReferenceFieldUpdater<BufferUntilChangedSubscriber, Subscription>
				S =
				AtomicReferenceFieldUpdater.newUpdater(BufferUntilChangedSubscriber.class,
						Subscription.class,
						"s");

		@Nullable
		K lastKey;

		public BufferUntilChangedSubscriber(CoreSubscriber<? super C> actual,
				Function<? super T, K> keyExtractor,
				BiPredicate<? super K, ? super K> keyComparator,
				C initialBuffer,
				Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.keyExtractor = keyExtractor;
			this.keyComparator = keyComparator;
			this.buffer = initialBuffer;
			this.bufferSupplier = bufferSupplier;
		}

		@Override
		public CoreSubscriber<? super C> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (!tryOnNext(t)) {
				s.request(1);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			Operators.onDiscardMultiple(buffer, actual.currentContext());
			buffer = null;
			lastKey = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			lastKey = null;
			DrainUtils.postComplete(actual, this, REQUESTED, this, this);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (n == Long.MAX_VALUE) {
					fastpath = true;
					requested = Long.MAX_VALUE;
					s.request(Long.MAX_VALUE);
				}
				else if (!DrainUtils.postCompleteRequest(n,
						actual,
						this,
						REQUESTED,
						this,
						this)) {
					s.request(1);
				}
			}
		}

		@Override
		public void cancel() {
			Operators.terminate(S, this);
			Operators.onDiscardMultiple(buffer, actual.currentContext());
			lastKey = null;
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			C b = buffer;
			K k;

			try {
				k = Objects.requireNonNull(keyExtractor.apply(t),
						"The distinct extractor returned a null value.");
			}
			catch (NullPointerException e) {
				Context ctx = actual.currentContext();
				onError(Operators.onOperatorError(s, e, t, ctx));
				Operators.onDiscardMultiple(buffer, ctx);
				Operators.onDiscard(t, ctx);
				return true;
			}

			if (null == lastKey) {
				lastKey = k;
				b.add(t);
				return true;
			}

			boolean match;

			match = keyComparator.test(lastKey, k);
			lastKey = k;

			boolean requestMore;
			if (match) {
				b.add(t);
			}
			else {
				requestMore = onNextNewBuffer();
				b = buffer;
				b.add(t);
				return !requestMore;
			}

			return !(!fastpath && requested != 0);
		}

		@Nullable
		C triggerNewBuffer() {
			C b = buffer;

			if (b.isEmpty()) {
				return null;
			}

			C c;

			try {
				c = Objects.requireNonNull(bufferSupplier.get(),
						"The bufferSupplier returned a null buffer");
			}
			catch (NullPointerException e) {
				onError(Operators.onOperatorError(s, e, actual.currentContext()));
				return null;
			}

			buffer = c;
			return b;
		}

		boolean onNextNewBuffer() {
			C b = triggerNewBuffer();
			if (b != null) {
				return emit(b);
			}
			return true;
		}

		boolean emit(C b) {
			if (fastpath) {
				actual.onNext(b);
				return false;
			}

			long r = REQUESTED.getAndDecrement(this);
			if (r > 0) {
				actual.onNext(b);
				return requested > 0;
			}

			cancel();
			actual.onError(Exceptions.failWithOverflow(
					"Could not emit buffer due to lack of requests"));

			return false;
		}

		@Override
		public boolean getAsBoolean() {
			return s == Operators.cancelledSubscription();
		}

		@Override
		public Iterator<C> iterator() {
			if (isEmpty()) {
				return Collections.emptyIterator();
			}
			return Collections.singleton(buffer).iterator();
		}

		@Override
		public boolean offer(C objects) {
			throw new IllegalArgumentException();
		}

		@Override
		@Nullable
		public C poll() {
			C b = buffer;
			if (b != null && !b.isEmpty()) {
				buffer = null;
				return b;
			}
			return null;
		}

		@Override
		@Nullable
		public C peek() {
			return buffer;
		}

		@Override
		public int size() {
			return buffer == null || buffer.isEmpty() ? 0 : 1;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return getAsBoolean();
			if (key == Attr.CAPACITY) {
				C b = buffer;
				return b != null ? b.size() : 0;
			}
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public String toString() {
			return "FluxBufferUntilChanged";
		}
	}
}
