/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.Disposable.Composite;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

/**
 * See https://github.com/reactor/reactor-core/issues/1237
 *
 * @author Simon Baslé
 */
@State(Scope.Benchmark)
@Fork(1)
public class CompositeDisposableHashcodeBenchmark {
	
	@Param({"1", "31", "1024", "10000"})
	public static int elementCount;

	private static class NativeHashCode implements Disposable {

		boolean isDisposed;

		@Override
		public void dispose() {
			this.isDisposed = true;
			Blackhole.consumeCPU(5);
		}

		@Override
		public boolean isDisposed() {
			return isDisposed;
		}
	}

	private static class CachingHashCode implements Disposable {

		final int hash;
		boolean isDisposed;

		public CachingHashCode() {
			this.hash = super.hashCode();
		}

		@Override
		public int hashCode() {
			return hash;
		}

		@Override
		public void dispose() {
			this.isDisposed = true;
			Blackhole.consumeCPU(5);
		}

		@Override
		public boolean isDisposed() {
			return isDisposed;
		}
	}

	@Benchmark()
	@BenchmarkMode(Mode.Throughput)
	public void setWithNativeHashcode(Blackhole bh) {
		Composite compositeSet = new SetCompositeDisposable();
		Disposable last = new NativeHashCode();
		for (int i = 0; i < elementCount; i++) {
			last = new NativeHashCode();
			compositeSet.add(last);
		}
		
		bh.consume(compositeSet.remove(last));
		bh.consume(compositeSet.remove(new NativeHashCode()));
	}

	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	public void setWithCachingHashcode(Blackhole bh) {
		Composite compositeSet = new SetCompositeDisposable();
		Disposable last = new CachingHashCode();
		for (int i = 0; i < elementCount; i++) {
			last = new NativeHashCode();
			compositeSet.add(last);
		}

		bh.consume(compositeSet.remove(last));
		bh.consume(compositeSet.remove(new NativeHashCode()));
	}

	@Benchmark()
	@BenchmarkMode(Mode.Throughput)
	public void listWithNativeHashcode(Blackhole bh) {
		Composite compositecompositeLight = Disposables.composite();
		Disposable last = new NativeHashCode();
		for (int i = 0; i < elementCount; i++) {
			last = new NativeHashCode();
			compositecompositeLight.add(last);
		}
		
		bh.consume(compositecompositeLight.remove(last));
		bh.consume(compositecompositeLight.remove(new NativeHashCode()));
	}

	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	public void listWithCachingHashcode(Blackhole bh) {
		Composite compositeLight = Disposables.composite();
		Disposable last = new CachingHashCode();
		for (int i = 0; i < elementCount; i++) {
			last = new NativeHashCode();
			compositeLight.add(last);
		}

		bh.consume(compositeLight.remove(last));
		bh.consume(compositeLight.remove(new CachingHashCode()));
	}

	/**
	 * == THIS CLASS IS AN OLD IMPLEMENTATION KEPT FOR THE SAKE OF THE JMH BENCHMARK ==
	 *
	 * A {@link Disposable.Composite} that allows to atomically add, remove and mass dispose.
	 *
	 * @author Simon Baslé
	 * @author Stephane Maldini
	 * @author David Karnok
	 */
	private static final class SetCompositeDisposable implements Disposable.Composite, Scannable {

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


}
