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

import java.util.Arrays;

import org.junit.Test;
import reactor.test.FakeDisposable;

import static org.assertj.core.api.Assertions.assertThat;

public class DisposableTest {

	@Test
	public void compositeDisposableAddAllDefault() {
		FakeDisposable d1 = new FakeDisposable();
		FakeDisposable d2 = new FakeDisposable();

		Disposable.Composite cd = new Disposable.Composite() {

			volatile boolean disposed;
			volatile int size;

			@Override
			public boolean add(Disposable d) {
				size++;
				return true;
			}

			@Override
			public boolean remove(Disposable d) {
				return false;
			}

			@Override
			public int size() {
				return size;
			}

			@Override
			public void dispose() {
				this.disposed = true;
			}

			@Override
			public boolean isDisposed() {
				return disposed;
			}
		};

		assertThat(cd.size()).isZero();

		boolean added = cd.addAll(Arrays.asList(d1, d2));

		assertThat(added).isTrue();
		assertThat(cd.size()).isEqualTo(2);
		assertThat(d1.isDisposed()).isFalse();
		assertThat(d2.isDisposed()).isFalse();
	}

	@Test
	public void compositeDisposableAddAllDefaultAfterDispose() throws Exception {
		final FakeDisposable d1 = new FakeDisposable();
		final FakeDisposable d2 = new FakeDisposable();

		Disposable.Composite cd = new Disposable.Composite() {

			volatile boolean disposed;
			volatile int size;

			@Override
			public boolean add(Disposable d) {
				size++;
				return true;
			}

			@Override
			public boolean remove(Disposable d) {
				return false;
			}

			@Override
			public int size() {
				return size;
			}

			@Override
			public void dispose() {
				this.disposed = true;
			}

			@Override
			public boolean isDisposed() {
				return disposed;
			}
		};

		cd.dispose();
		boolean added = cd.addAll(Arrays.asList(d1, d2));

		assertThat(added).isFalse();
		assertThat(cd.size()).isZero();
		assertThat(d1.isDisposed()).isTrue();
		assertThat(d2.isDisposed()).isTrue();
	}

	@Test
	public void compositeDisposableAddAllDefaultDuringDispose() throws Exception {
		final FakeDisposable d1 = new FakeDisposable();
		final FakeDisposable d2 = new FakeDisposable();
		final FakeDisposable d3 = new FakeDisposable();

		Disposable.Composite cd = new Disposable.Composite() {

			volatile boolean disposed;
			volatile int size;

			@Override
			public boolean add(Disposable d) {
				if (d == d2) {
					disposed = true;
				}

				if (disposed) {
					d.dispose();
					return false;
				}
				size++;
				return true;
			}

			@Override
			public boolean remove(Disposable d) {
				return false;
			}

			@Override
			public int size() {
				return size;
			}

			@Override
			public void dispose() {
				this.disposed = true;
			}

			@Override
			public boolean isDisposed() {
				return disposed;
			}
		};

		boolean added = cd.addAll(Arrays.asList(d1, d2, d3));

		assertThat(added).isFalse();
		//not atomic: first element added
		assertThat(cd.size()).isEqualTo(1);
		assertThat(d1.isDisposed()).isFalse();
		assertThat(d2.isDisposed()).isTrue();
		assertThat(d3.isDisposed()).isTrue();
	}

}