/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.Disposable.Composite;

/**
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
		Composite compositeSet = new Disposables.SetCompositeDisposable();
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
		Composite compositeSet = new Disposables.SetCompositeDisposable();
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

}
