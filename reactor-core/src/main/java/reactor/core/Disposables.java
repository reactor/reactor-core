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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

/**
 * A support class that offers factory methods for implementations of the specialized
 * {@link Disposable} sub-interfaces ({@link Disposable.Composite Disposable.Composite},
 * {@link Disposable.Swap Disposable.Swap}).
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
public final class Disposables {

	private Disposables() { }

	/**
	 * Create a new empty {@link Disposable.Composite} with atomic guarantees on all mutative
	 * operations.
	 *
	 * @return an empty atomic {@link Disposable.Composite}
	 */
	public static Disposable.Composite composite() {
		return new ListCompositeDisposable();
	}

	/**
	 * Create and initialize a new {@link Disposable.Composite} with atomic guarantees on
	 * all mutative operations.
	 *
	 * @return a pre-filled atomic {@link Disposable.Composite}
	 */
	public static Disposable.Composite composite(Disposable... disposables) {
		return new ListCompositeDisposable(disposables);
	}

	/**
	 * Create and initialize a new {@link Disposable.Composite} with atomic guarantees on
	 * all mutative operations.
	 *
	 * @return a pre-filled atomic {@link Disposable.Composite}
	 */
	public static Disposable.Composite composite(
			Iterable<? extends Disposable> disposables) {
		return new ListCompositeDisposable(disposables);
	}

	/**
	 * Return a new {@link Disposable} that is already disposed.
	 *
	 * @return a new disposed {@link Disposable}.
	 */
	public static Disposable disposed() {
		return new AlwaysDisposable();
	}

	/**
	 * Return a new {@link Disposable} that can never be disposed. Calling {@link Disposable#dispose()}
	 * is a NO-OP and {@link Disposable#isDisposed()} always return false.
	 *
	 * @return a new {@link Disposable} that can never be disposed.
	 */
	public static Disposable never() {
		return new NeverDisposable();
	}

	/**
	 * Return a new simple {@link Disposable} instance that is initially not disposed but
	 * can be by calling {@link Disposable#dispose()}.
	 *
	 * @return a new {@link Disposable} initially not yet disposed.
	 */
	public static Disposable single() {
		return new SimpleDisposable();
	}

	/**
	 * Create a new empty {@link Disposable.Swap} with atomic guarantees on all mutative
	 * operations.
	 *
	 * @return an empty atomic {@link Disposable.Swap}
	 */
	public static Disposable.Swap swap() {
		return new SwapDisposable();
	}

	//==== STATIC package private implementations ====

	/**
	 * A {@link Disposable.Composite} that allows to atomically add, remove and mass dispose.
	 *
	 * @author Simon Baslé
	 * @author Stephane Maldini
	 * @author David Karnok
	 */
	static final class SetCompositeDisposable implements Disposable.Composite, Scannable {

		static final int   DEFAULT_CAPACITY    = 16;
		static final float DEFAULT_LOAD_FACTOR = 0.75f;

		final float loadFactor;
		int          mask;
		int          size;
		int          maxSize;
		Disposable[] disposables;

		volatile boolean disposed;

		/**
		 * Creates an empty {@link SetCompositeDisposable}.
		 */
		SetCompositeDisposable() {
			this.loadFactor = DEFAULT_LOAD_FACTOR;
			int c = DEFAULT_CAPACITY;
			this.mask = c - 1;
			this.maxSize = (int) (loadFactor * c);
			this.disposables = new Disposable[c];
		}

		/**
		 * Creates a {@link SetCompositeDisposable} with the given array of initial elements.
		 * @param disposables the array of {@link Disposable} to start with
		 */
		SetCompositeDisposable(Disposable... disposables) {
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
		 * Creates a {@link SetCompositeDisposable} with the given {@link Iterable} sequence of
		 * initial elements.
		 * @param disposables the Iterable sequence of {@link Disposable} to start with
		 */
		SetCompositeDisposable(Iterable<? extends Disposable> disposables) {
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
	 * @author David Karnok
	 * @author Simon Baslé
	 */
	static final class ListCompositeDisposable implements Disposable.Composite {

		@Nullable
		List<Disposable> resources;

		volatile boolean disposed;

		ListCompositeDisposable() {
		}

		ListCompositeDisposable(Disposable... resources) {
			Objects.requireNonNull(resources, "resources is null");
			this.resources = new LinkedList<>();
			for (Disposable d : resources) {
				Objects.requireNonNull(d, "Disposable item is null");
				this.resources.add(d);
			}
		}

		ListCompositeDisposable(Iterable<? extends Disposable> resources) {
			Objects.requireNonNull(resources, "resources is null");
			this.resources = new LinkedList<>();
			for (Disposable d : resources) {
				Objects.requireNonNull(d, "Disposable item is null");
				this.resources.add(d);
			}
		}

		@Override
		public void dispose() {
			if (disposed) {
				return;
			}
			List<Disposable> set;
			synchronized (this) {
				if (disposed) {
					return;
				}
				disposed = true;
				set = resources;
				resources = null;
			}

			dispose(set);
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
						List<Disposable> set = resources;
						if (set == null) {
							set = new LinkedList<>();
							resources = set;
						}
						set.add(d);
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
						List<Disposable> set = resources;
						if (set == null) {
							set = new LinkedList<>();
							resources = set;
						}
						for (Disposable d : ds) {
							Objects.requireNonNull(d, "d is null");
							set.add(d);
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

				List<Disposable> set = resources;
				if (set == null || !set.remove(d)) {
					return false;
				}
			}
			return true;
		}

		@Override
		public int size() {
			List<Disposable> r = resources;
			return r == null ? 0 : r.size();
		}

		public void clear() {
			if (disposed) {
				return;
			}
			List<Disposable> set;
			synchronized (this) {
				if (disposed) {
					return;
				}

				set = resources;
				resources = null;
			}

			dispose(set);
		}

		void dispose(@Nullable List<Disposable> set) {
			if (set == null) {
				return;
			}
			List<Throwable> errors = null;
			for (Disposable o : set) {
				try {
					o.dispose();
				} catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					if (errors == null) {
						errors = new ArrayList<>();
					}
					errors.add(ex);
				}
			}
			if (errors != null) {
				if (errors.size() == 1) {
					throw Exceptions.propagate(errors.get(0));
				}
				throw Exceptions.multiple(errors);
			}
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
	public boolean update(@Nullable Disposable next) {
		return Disposables.set(INNER, this, next);
	}

	@Override
	public boolean replace(@Nullable Disposable next) {
		return Disposables.replace(INNER, this, next);
	}

	@Override
	@Nullable
	public Disposable get() {
		return inner;
	}

	@Override
	public void dispose() {
		Disposables.dispose(INNER, this);
	}

	@Override
	public boolean isDisposed() {
		return Disposables.isDisposed(INNER.get(this));
	}
}

/**
 * A very simple {@link Disposable} that only wraps a mutable boolean for
 * {@link #isDisposed()}.
 */
static final class SimpleDisposable extends AtomicBoolean implements Disposable {

	@Override
	public void dispose() {
		set(true);
	}

	@Override
	public boolean isDisposed() {
		return get();
	}
}

/**
 * Immutable disposable that is always {@link #isDisposed() disposed}. Calling
 * {@link #dispose()} does nothing, and {@link #isDisposed()} always return true.
 */
static final class AlwaysDisposable implements Disposable {

	@Override
	public void dispose() {
		//NO-OP
	}

	@Override
	public boolean isDisposed() {
		return true;
	}
}

/**
 * Immutable disposable that is never {@link #isDisposed() disposed}. Calling
 * {@link #dispose()} does nothing, and {@link #isDisposed()} always return false.
 */
static final class NeverDisposable implements Disposable {

	@Override
	public void dispose() {
		//NO-OP
	}

	@Override
	public boolean isDisposed() {
		return false;
	}
}

	//==== STATIC ATOMIC UTILS copied from Disposables ====

	/**
	 * A singleton {@link Disposable} that represents a disposed instance. Should not be
	 * leaked to clients.
	 */
	//NOTE: There is a private similar DISPOSED singleton in Disposables as well
	static final Disposable DISPOSED = disposed();

	/**
	 * Atomically set the field to a {@link Disposable} and dispose the old content.
	 *
	 * @param updater the target field updater
	 * @param holder the target instance holding the field
	 * @param newValue the new Disposable to set
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
	 * @param newValue the new Disposable to set, null allowed
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
