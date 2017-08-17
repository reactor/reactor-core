/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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
import javax.annotation.Nullable;

import reactor.util.concurrent.OpenHashSet;

/**
 * A support class that offers implementations for the specialized {@link Disposable}
 * sub-interfaces ({@link Disposable.Composite Disposable.CompositeDisposable},
 * {@link Disposable.Sequential Disposable.SequentialDisposable}).
 *
 * @author Simon Baslé
 */
public final class DefaultDisposable {

	DefaultDisposable() { }

	/**
	 * Create a new empty {@link Disposable.Sequential} with atomic guarantees on all mutative
	 * operations.
	 *
	 * @return an empty atomic {@link Disposable.Sequential}
	 */
	public static Disposable.Sequential sequential() {
		return new AtomicSequentialDisposable();
	}

	/**
	 * Create a new empty {@link Disposable.Composite} with atomic guarantees on all mutative
	 * operations.
	 *
	 * @return an empty atomic {@link Disposable.Composite}
	 */
	public static Disposable.Composite<Disposable> composite() {
		return new AtomicCompositeDisposable();
	}

	/**
	 * Create and initialize a new {@link Disposable.Composite} with atomic guarantees on
	 * all mutative operations.
	 *
	 * @return a pre-filled atomic {@link Disposable.Composite}
	 */
	public static Disposable.Composite<Disposable> compositeOf(Disposable... disposables) {
		return new AtomicCompositeDisposable(disposables);
	}

	/**
	 * Create and initialize a new {@link Disposable.Composite} with atomic guarantees on
	 * all mutative operations.
	 *
	 * @return a pre-filled atomic {@link Disposable.Composite}
	 */
	public static Disposable.Composite<Disposable> compositeOf(Iterable<? extends Disposable> disposables) {
		return new AtomicCompositeDisposable(disposables);
	}

	//==== PACKAGE-PRIVATE IMPLEMENTATIONS ====

	/**
	 * A {@link Disposable.Composite} that allows to atomically add, remove and mass dispose.
	 *
	 * @author Simon Baslé
	 * @author David Karnok
	 */
	static final class AtomicCompositeDisposable implements
	                                             Disposable.Composite<Disposable> {

		OpenHashSet<Disposable> disposables;
		volatile boolean disposed;

		/**
		 * Creates an empty {@link AtomicCompositeDisposable}.
		 */
		public AtomicCompositeDisposable() {
		}

		/**
		 * Creates a {@link AtomicCompositeDisposable} with the given array of initial elements.
		 * @param disposables the array of {@link Disposable} to start with
		 */
		public AtomicCompositeDisposable(Disposable... disposables) {
			Objects.requireNonNull(disposables, "disposables is null");
			this.disposables = new OpenHashSet<>(disposables.length + 1, 0.75f);
			for (Disposable d : disposables) {
				Objects.requireNonNull(d, "Disposable item is null");
				this.disposables.add(d);
			}
		}

		/**
		 * Creates a {@link AtomicCompositeDisposable} with the given {@link Iterable} sequence of
		 * initial elements.
		 * @param disposables the Iterable sequence of {@link Disposable} to start with
		 */
		public AtomicCompositeDisposable(Iterable<? extends Disposable> disposables) {
			Objects.requireNonNull(disposables, "disposables is null");
			this.disposables = new OpenHashSet<>();
			for (Disposable d : disposables) {
				Objects.requireNonNull(d, "Disposable item is null");
				this.disposables.add(d);
			}
		}

		@Override
		public void dispose() {
			if (disposed) {
				return;
			}
			OpenHashSet<Disposable> set;
			synchronized (this) {
				if (disposed) {
					return;
				}
				disposed = true;
				set = disposables;
				disposables = null;
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
						OpenHashSet<Disposable> set = disposables;
						if (set == null) {
							set = new OpenHashSet<>();
							disposables = set;
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
						OpenHashSet<Disposable> set = disposables;
						if (set == null) {
							set = new OpenHashSet<>(ds.size() + 1, 0.75f);
							disposables = set;
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

				OpenHashSet<Disposable> set = disposables;
				if (set == null || !set.remove(d)) {
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
				OpenHashSet<Disposable> set = disposables;
				return set != null ? set.size() : 0;
			}
		}

		/**
		 * Dispose the contents of the OpenHashSet by suppressing non-fatal
		 * Throwables till the end.
		 * @param set the OpenHashSet to dispose elements of
		 */
		void dispose(@Nullable OpenHashSet<Disposable> set) {
			if (set == null) {
				return;
			}
			List<Throwable> errors = null;
			Object[] array = set.keys();
			for (Object o : array) {
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
	}

	/**
	 * Not public API. Implementation of a {@link Disposable.Sequential}.
	 *
	 * @author Simon Baslé
	 * @author David Karnok
	 */
	static final class AtomicSequentialDisposable implements Disposable.Sequential {

		volatile Disposable inner;
		static final AtomicReferenceFieldUpdater<AtomicSequentialDisposable, Disposable>
				INNER =
				AtomicReferenceFieldUpdater.newUpdater(AtomicSequentialDisposable.class, Disposable.class, "inner");

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
