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

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.test.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class DisposablesTest {

	static final AtomicReferenceFieldUpdater<TestDisposable, Disposable> DISPOSABLE_UPDATER =
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
		assertThat(Disposables.DISPOSED).isNotSameAs(Disposable.disposed());
	}

	@Test
	public void validationNull() {
		Hooks.onErrorDropped(e -> assertThat(e).isInstanceOf(NullPointerException.class)
		                                       .hasMessage("next is null"));
		try {
			assertThat(Disposables.validate(null, null, Operators::onErrorDropped)).isFalse();
		} finally {
			Hooks.resetOnErrorDropped();
		}
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

			RaceTestUtils.race(r, r, Schedulers.elastic());
		}
	}

	@Test
	public void setReplace() {
		for (int i = 0; i < 500; i++) {

			TestDisposable r = new TestDisposable() {
				@Override
				public void run() {
					Disposables.replace(DISPOSABLE_UPDATER, this, Disposable.empty());
				}
			};

			RaceTestUtils.race(r, r, Schedulers.elastic());
		}
	}

	@Test
	public void setRace() {
		for (int i = 0; i < 500; i++) {
			TestDisposable r = new TestDisposable() {
				@Override
				public void run() {
					Disposables.set(DISPOSABLE_UPDATER, this, Disposable.empty());
				}
			};

			RaceTestUtils.race(r, r, Schedulers.elastic());
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
		Disposable u = Disposable.empty();
		TestDisposable r = new TestDisposable(u);

		Disposables.dispose(DISPOSABLE_UPDATER, r);

		assertThat(u.isDisposed()).isTrue();
	}

	@Test
	public void trySet() {
		TestDisposable r = new TestDisposable();

		Disposable d1 = Disposable.empty();

		assertThat(Disposables.trySet(DISPOSABLE_UPDATER, r, d1)).isTrue();

		Disposable d2 = Disposable.empty();

		assertThat(Disposables.trySet(DISPOSABLE_UPDATER, r, d2)).isFalse();

		assertThat(d1.isDisposed()).isFalse();

		assertThat(d2.isDisposed()).isFalse();

		Disposables.dispose(DISPOSABLE_UPDATER, r);

		Disposable d3 = Disposable.empty();

		assertThat(Disposables.trySet(DISPOSABLE_UPDATER, r, d3)).isFalse();

		assertThat(d3.isDisposed()).isTrue();
	}
}