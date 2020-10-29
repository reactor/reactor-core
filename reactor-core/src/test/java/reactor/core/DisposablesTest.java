/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.FakeDisposable;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class DisposablesTest {

	//==== PUBLIC API TESTS ====

	@Test
	public void sequentialEmpty() {
		assertThat(Disposables.swap()
		                      .get()).isNull();
	}

	@Test
	public void compositeEmpty() {
		Disposable.Composite cd = Disposables.composite();
		assertThat(cd.size()).isZero();
		assertThat(cd.isDisposed()).isFalse();
	}

	@Test
	public void compositeFromArray() {
		Disposable d1 = new FakeDisposable();
		Disposable d2 = new FakeDisposable();

		Disposable.Composite cd = Disposables.composite(d1, d2);
		assertThat(cd.size()).isEqualTo(2);
		assertThat(cd.isDisposed()).isFalse();
	}

	@Test
	public void compositeFromCollection() {
		Disposable d1 = new FakeDisposable();
		Disposable d2 = new FakeDisposable();

		Disposable.Composite cd = Disposables.composite(Arrays.asList(d1, d2));
		assertThat(cd.size()).isEqualTo(2);
		assertThat(cd.isDisposed()).isFalse();
	}

	//==== PRIVATE API TESTS ====

	static final AtomicReferenceFieldUpdater<TestDisposable, Disposable>
			DISPOSABLE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(TestDisposable.class, Disposable.class, "disp");

	private static class TestDisposable implements Runnable {
		volatile Disposable disp;

		public TestDisposable() {
		}

		public TestDisposable(Disposable disp) {
			this.disp = disp;
		}

		@Override
		public void run() {
			//NO-OP by default
		}
	}


	@Test
	public void singletonIsDisposed() {
		assertThat(Disposables.DISPOSED.isDisposed()).isTrue();
		Disposables.DISPOSED.dispose();
		assertThat(Disposables.DISPOSED.isDisposed()).isTrue();
		assertThat(Disposables.DISPOSED).isNotSameAs(Disposables.disposed());
	}

	@Test
	public void disposeRace() {
		for (int i = 0; i < 500; i++) {

			TestDisposable r = new TestDisposable() {
				@Override
				public void run() {
					Disposables.dispose(DISPOSABLE_UPDATER, this);
				}
			};

			RaceTestUtils.race(r, r, Schedulers.boundedElastic());
		}
	}

	@Test
	public void setReplace() {
		for (int i = 0; i < 500; i++) {

			TestDisposable r = new TestDisposable() {
				@Override
				public void run() {
					Disposables.replace(DISPOSABLE_UPDATER, this, Disposables.single());
				}
			};

			RaceTestUtils.race(r, r, Schedulers.boundedElastic());
		}
	}

	@Test
	public void setRace() {
		for (int i = 0; i < 500; i++) {
			TestDisposable r = new TestDisposable() {
				@Override
				public void run() {
					Disposables.set(DISPOSABLE_UPDATER, this, Disposables.single());
				}
			};

			RaceTestUtils.race(r, r, Schedulers.boundedElastic());
		}
	}

	@Test
	public void setReplaceNull() {
		TestDisposable r = new TestDisposable();

		Disposables.dispose(DISPOSABLE_UPDATER, r);

		assertThat(Disposables.set(DISPOSABLE_UPDATER, r, null)).isFalse();
		assertThat(Disposables.replace(DISPOSABLE_UPDATER, r, null)).isFalse();
	}

	@Test
	public void dispose() {
		Disposable u = Disposables.single();
		TestDisposable r = new TestDisposable(u);

		Disposables.dispose(DISPOSABLE_UPDATER, r);

		assertThat(u.isDisposed()).isTrue();
	}

}
