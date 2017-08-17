/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import reactor.util.concurrent.Queues;

/**
 * A support class that offers implementations for the specialized {@link Disposable}
 * sub-interfaces ({@link Disposable.Composite Disposable.CompositeDisposable},
 * {@link Disposable.Swap Disposable.SwapDisposable}).
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
abstract class DefaultDisposable {

	DefaultDisposable() { }

	/**
	 * A {@link Disposable.Composite} that allows to atomically add, remove and mass dispose.
	 *
	 * @author Simon Baslé
	 * @author Stephane Maldini
	 * @author David Karnok
	 */
	static final class CompositeDisposable implements Disposable.Composite, Scannable {

		static final int   DEFAULT_CAPACITY    = 16;
		static final float DEFAULT_LOAD_FACTOR = 0.75f;

		final float loadFactor;
		int          mask;
		int          size;
		int          maxSize;
		Disposable[] disposables;

		volatile boolean disposed;

		/**
		 * Creates an empty {@link CompositeDisposable}.
		 */
		CompositeDisposable() {
			this.loadFactor = DEFAULT_LOAD_FACTOR;
			int c = DEFAULT_CAPACITY;
			this.mask = c - 1;
			this.maxSize = (int) (loadFactor * c);
			this.disposables = new Disposable[c];
		}

		/**
		 * Creates a {@link CompositeDisposable} with the given array of initial elements.
		 * @param disposables the array of {@link Disposable} to start with
		 */
		CompositeDisposable(Disposable... disposables) {
			Objects.requireNonNull(disposables, "disposables is null");

			int capacity = disposables.length + 1;
			this.loadFactor = DEFAULT_LOAD_FACTOR;
			int c = Queues.ceilingNextPowerOfTwo(capacity);
			this.mask = c - 1;
			this.maxSize = (int) (loadFactor * c);
			this.disposables = new Disposable[c];

			for (Disposable d : disposables) {
				Objects.requireNonNull(d, "Disposable item is null");
				addEntry(d);
			}
		}

		/**
		 * Creates a {@link CompositeDisposable} with the given {@link Iterable} sequence of
		 * initial elements.
		 * @param disposables the Iterable sequence of {@link Disposable} to start with
		 */
		CompositeDisposable(Iterable<? extends Disposable> disposables) {
			Objects.requireNonNull(disposables, "disposables is null");
			this.loadFactor = DEFAULT_LOAD_FACTOR;
			int c = DEFAULT_CAPACITY;
			this.mask = c - 1;
			this.maxSize = (int) (loadFactor * c);
			this.disposables = new Disposable[c];
			for (Disposable d : disposables) {
				Objects.requireNonNull(d, "Disposable item is null");
				addEntry(d);
			}
		}

		@Override
		public void dispose() {
			if (disposed) {
				return;
			}
			Disposable[] set;
			synchronized (this) {
				if (disposed) {
					return;
				}
				disposed = true;
				set = disposables;
				disposables = null;
				size = 0;
			}

			List<Throwable> errors = null;
			for (Object o : set) {
				if (o instanceof Disposable) {
					try {
						((Disposable) o).dispose();
					} catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						if (errors == null) {
							errors = new ArrayList<>();
						}
						errors.add(ex);
					}
				}
			}
			if (errors != null) {
				if (errors.size() == 1) {
					throw Exceptions.propagate(errors.get(0));
				}
				throw Exceptions.multiple(errors);
			}
		}

		@Override
		public boolean isDisposed() {
			return disposed;
		}

		@Override
		public boolean add(Disposable d) {
			Objects.requireNonNull(d, "d is null");
			if (!disposed) {
				synchronized (this) {
					if (!disposed) {
						addEntry(d);
						return true;
					}
				}
			}
			d.dispose();
			return false;
		}

		@Override
		public boolean addAll(Collection<? extends Disposable> ds) {
			Objects.requireNonNull(ds, "ds is null");
			if (!disposed) {
				synchronized (this) {
					if (!disposed) {
						for (Disposable d : ds) {
							Objects.requireNonNull(d, "d is null");
							addEntry(d);
						}
						return true;
					}
				}
			}
			for (Disposable d : ds) {
				d.dispose();
			}
			return false;
		}

		@Override
		public boolean remove(Disposable d) {
			Objects.requireNonNull(d, "Disposable item is null");
			if (disposed) {
				return false;
			}
			synchronized (this) {
				if (disposed) {
					return false;
				}

				if (!removeEntry(d)) {
					return false;
				}
			}
			return true;
		}

		@Override
		public int size() {
			if (disposed) {
				return 0;
			}
			synchronized (this) {
				if (disposed) {
					return 0;
				}
				return size;
			}
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(disposables)
			             .filter(Objects::nonNull)
			             .map(Scannable::from);
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) {
				return isDisposed();
			}
			return null;
		}

		boolean addEntry(Disposable value) {
			final Disposable[] a = disposables;
			final int m = mask;

			int pos = mix(value.hashCode()) & m;
			Disposable curr = a[pos];
			if (curr != null) {
				if (curr.equals(value)) {
					return false;
				}
				for (;;) {
					pos = (pos + 1) & m;
					curr = a[pos];
					if (curr == null) {
						break;
					}
					if (curr.equals(value)) {
						return false;
					}
				}
			}
			a[pos] = value;
			if (++size >= maxSize) {
				rehash();
			}
			return true;
		}


		boolean removeEntry(Disposable value) {
			Disposable[] a = disposables;
			int m = mask;
			int pos = mix(value.hashCode()) & m;
			Disposable curr = a[pos];
			if (curr == null) {
				return false;
			}
			if (curr.equals(value)) {
				return removeEntry(pos, a, m);
			}
			for (;;) {
				pos = (pos + 1) & m;
				curr = a[pos];
				if (curr == null) {
					return false;
				}
				if (curr.equals(value)) {
					return removeEntry(pos, a, m);
				}
			}
		}

		boolean removeEntry(int pos, Disposable[] a, int m) {
			size--;

			int last;
			int slot;
			Disposable curr;
			for (;;) {
				last = pos;
				pos = (pos + 1) & m;
				for (;;) {
					curr = a[pos];
					if (curr == null) {
						a[last] = null;
						return true;
					}
					slot = mix(curr.hashCode()) & m;

					if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
						break;
					}

					pos = (pos + 1) & m;
				}
				a[last] = curr;
			}
		}

		static final int INT_PHI = 0x9E3779B9;

		void rehash() {
			Disposable[] a = disposables;
			int i = a.length;
			int newCap = i << 1;
			int m = newCap - 1;

			Disposable[] b = new Disposable[newCap];


			for (int j = size; j-- != 0; ) {
				while (a[--i] == null);
				int pos = mix(a[i].hashCode()) & m;
				if (b[pos] != null) {
					for (;;) {
						pos = (pos + 1) & m;
						if (b[pos] == null) {
							break;
						}
					}
				}
				b[pos] = a[i];
			}

			this.mask = m;
			this.maxSize = (int)(newCap * loadFactor);
			this.disposables = b;
		}

		static int mix(int x) {
			final int h = x * INT_PHI;
			return h ^ (h >>> 16);
		}
	}

	/**
	 * Not public API. Implementation of a {@link Swap}.
	 *
	 * @author Simon Baslé
	 * @author David Karnok
	 */
	static final class SwapDisposable implements Disposable.Swap {

		volatile Disposable inner;
		static final AtomicReferenceFieldUpdater<SwapDisposable, Disposable>
				INNER =
				AtomicReferenceFieldUpdater.newUpdater(SwapDisposable.class, Disposable.class, "inner");

		@Override
		public boolean update(Disposable next) {
			return DefaultDisposable.set(INNER, this, next);
		}

		@Override
		public boolean replace(@Nullable Disposable next) {
			return DefaultDisposable.replace(INNER, this, next);
		}

		@Override
		@Nullable
		public Disposable get() {
			return inner;
		}

		@Override
		public void dispose() {
			DefaultDisposable.dispose(INNER, this);
		}

		@Override
		public boolean isDisposed() {
			return DefaultDisposable.isDisposed(INNER.get(this));
		}
	}


	//==== STATIC ATOMIC UTILS copied from Disposables ====

	static final Disposable DISPOSED = new Disposable() {
		@Override
		public void dispose() {
			//NO-OP
		}

		@Override
		public boolean isDisposed() {
			return true;
		}
	};

	/**
	 * Atomically push the field to a {@link Disposable} and dispose the old content.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @param newValue the new Disposable to push
	 * @return true if successful, false if the field contains the {@link #DISPOSED} instance.
	 */
	static <T> boolean set(AtomicReferenceFieldUpdater<T, Disposable> updater, T holder, @Nullable Disposable newValue) {
		for (;;) {
			Disposable current = updater.get(holder);
			if (current == DISPOSED) {
				if (newValue != null) {
					newValue.dispose();
				}
				return false;
			}
			if (updater.compareAndSet(holder, current, newValue)) {
				if (current != null) {
					current.dispose();
				}
				return true;
			}
		}
	}

	/**
	 * Atomically replace the {@link Disposable} in the field with the given new Disposable
	 * but do not dispose the old one.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @param newValue the new Disposable to push, null allowed
	 * @return true if the operation succeeded, false if the target field contained
	 * the common {@link #DISPOSED} instance and the given disposable is not null but is disposed.
	 */
	static <T> boolean replace(AtomicReferenceFieldUpdater<T, Disposable> updater, T holder, @Nullable Disposable newValue) {
		for (;;) {
			Disposable current = updater.get(holder);
			if (current == DISPOSED) {
				if (newValue != null) {
					newValue.dispose();
				}
				return false;
			}
			if (updater.compareAndSet(holder, current, newValue)) {
				return true;
			}
		}
	}

	/**
	 * Atomically dispose the {@link Disposable} in the field if not already disposed.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @return true if the {@link Disposable} held by the field was properly disposed
	 */
	static <T> boolean dispose(AtomicReferenceFieldUpdater<T, Disposable> updater, T holder) {
		Disposable current = updater.get(holder);
		Disposable d = DISPOSED;
		if (current != d) {
			current = updater.getAndSet(holder, d);
			if (current != d) {
				if (current != null) {
					current.dispose();
				}
				return true;
			}
		}
		return false;
	}

	/**
	 * Check if the given {@link Disposable} is the singleton {@link #DISPOSED}.
	 *
	 * @param d the disposable to check
	 * @return true if d is {@link #DISPOSED}
	 */
	static boolean isDisposed(Disposable d) {
		return d == DISPOSED;
	}
	//=====================================================
}
