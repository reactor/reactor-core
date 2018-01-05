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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import reactor.core.Disposables.CompositeDisposable;
import reactor.core.scheduler.Schedulers;
import reactor.test.FakeDisposable;
import reactor.test.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class CompositeDisposableTest {

	@Test
	public void isDisposed() throws Exception {
		Disposable.Composite cd = new CompositeDisposable();

		assertThat(cd.isDisposed()).isFalse();

		cd.dispose();
		assertThat(cd.isDisposed()).isTrue();
	}

	@Test
	public void add() throws Exception {
		FakeDisposable d = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable();

		assertThat(cd.size()).isZero();

		boolean added = cd.add(d);

		assertThat(added).isTrue();
		assertThat(cd.size()).isEqualTo(1);
		assertThat(d.isDisposed()).isFalse();
	}

	@Test
	public void addAll() throws Exception {
		FakeDisposable d1 = new FakeDisposable();
		FakeDisposable d2 = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable();

		assertThat(cd.size()).isZero();

		boolean added = cd.addAll(Arrays.asList(d1, d2));

		assertThat(added).isTrue();
		assertThat(cd.size()).isEqualTo(2);
		assertThat(d1.isDisposed()).isFalse();
		assertThat(d2.isDisposed()).isFalse();
	}

	@Test
	public void removeDoesntDispose() throws Exception {
		FakeDisposable d = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable(d);

		assertThat(cd.size()).isEqualTo(1);
		assertThat(d.isDisposed()).isFalse();

		boolean deleted = cd.remove(d);

		assertThat(deleted).isTrue();
		assertThat(cd.size()).isZero();
		assertThat(d.isDisposed()).isFalse();
	}

	@Test
	public void disposeDisposesAndDisallowReuse() throws Exception {
		FakeDisposable d1 = new FakeDisposable();
		FakeDisposable d2 = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable(d1, d2);

		assertThat(cd.size()).isEqualTo(2);
		assertThat(d1.isDisposed()).isFalse();
		assertThat(d2.isDisposed()).isFalse();

		cd.dispose();

		assertThat(cd.size()).isZero();
		assertThat(cd.isDisposed()).isTrue();
		assertThat(d1.isDisposed()).isTrue();
		assertThat(d1.disposed).isEqualTo(1);
		assertThat(d2.isDisposed()).isTrue();

		boolean reuse = cd.add(d1);
		assertThat(reuse).isFalse();
		assertThat(cd.size()).isZero();
		assertThat(d1.disposed).isEqualTo(2);
	}

	@Test
	public void removeInexistant() throws Exception {
		FakeDisposable d = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable();
		boolean deleted = cd.remove(d);

		assertThat(deleted).isFalse();
		assertThat(d.isDisposed()).isFalse();
	}

	@Test
	public void addAfterDispose() throws Exception {
		FakeDisposable d = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable();
		cd.dispose();
		boolean added = cd.add(d);

		assertThat(added).isFalse();
		assertThat(cd.size()).isZero();
		assertThat(d.isDisposed()).isTrue();
	}

	@Test
	public void addAllAfterDispose() throws Exception {
		FakeDisposable d1 = new FakeDisposable();
		FakeDisposable d2 = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable();
		cd.dispose();
		boolean added = cd.addAll(Arrays.asList(d1, d2));

		assertThat(added).isFalse();
		assertThat(cd.size()).isZero();
		assertThat(d1.isDisposed()).isTrue();
		assertThat(d2.isDisposed()).isTrue();
	}

	@Test
	public void removeAfterDispose() throws Exception {
		FakeDisposable d = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable();
		cd.dispose();
		boolean deleted = cd.remove(d);

		assertThat(deleted).isFalse();
		assertThat(cd.size()).isZero();
		assertThat(d.isDisposed()).isFalse();
	}

	@Test
	public void disposeAfterDispose() throws Exception {
		FakeDisposable d = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable(d);
		cd.dispose();
		cd.dispose();

		assertThat(d.disposed).isEqualTo(1);
	}

	@Test
	public void singleErrorDuringDisposal() {
		Disposable bad = () -> {
			throw new IllegalStateException("boom");
		};
		FakeDisposable good = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable(bad, good);

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(cd::dispose)
		                                                      .withMessage("boom");

		assertThat(good.isDisposed()).isTrue();
	}

	@Test
	public void multipleErrorsDuringDisposal() {
		Disposable bad1 = () -> {
			throw new IllegalStateException("boom1");
		};
		Disposable bad2 = () -> {
			throw new IllegalStateException("boom2");
		};
		FakeDisposable good = new FakeDisposable();
		Disposable.Composite cd = new CompositeDisposable(bad1, bad2, good);

		assertThatExceptionOfType(RuntimeException.class).isThrownBy(cd::dispose)
		                                                 .withMessage(
				                                                 "Multiple exceptions")
		                                                 .withStackTraceContaining(
				                                                 "Suppressed: java.lang.IllegalStateException: boom1")
		                                                 .withStackTraceContaining(
				                                                 "Suppressed: java.lang.IllegalStateException: boom2");

		assertThat(good.isDisposed()).isTrue();
	}

	@Test
	public void constructorIterable() {
		FakeDisposable d1 = new FakeDisposable();
		FakeDisposable d2 = new FakeDisposable();
		List<FakeDisposable> list = Arrays.asList(d1, d2);
		Disposable.Composite cd = new CompositeDisposable(list);

		assertThat(cd.size()).isEqualTo(2);

		cd.remove(d1);
		cd.remove(d2);

		assertThat(cd.size()).isZero();
		assertThat(list).hasSize(2);
	}

	@Test
	public void disposeConcurrent() {
		for (int i = 0; i < 500; i++) {
			final Disposable d1 = new FakeDisposable();
			final Disposable.Composite cd = new CompositeDisposable(d1);

			RaceTestUtils.race(cd::dispose, cd::dispose, Schedulers.elastic());
		}
	}

	@Test
	public void removeConcurrent() {
		for (int i = 0; i < 500; i++) {
			final Disposable d1 = new FakeDisposable();
			final Disposable.Composite cd = new CompositeDisposable(d1);

			RaceTestUtils.race(() -> cd.remove(d1), cd::dispose, Schedulers.elastic());
		}
	}

	@Test
	public void sizeConcurrent() {
		for (int i = 0; i < 500; i++) {
			final Disposable d1 = new FakeDisposable();
			final Disposable.Composite cd = new CompositeDisposable(d1);

			RaceTestUtils.race(cd::size, cd::dispose, Schedulers.elastic());
		}
	}

	@Test
	public void forEachTyped() {
		FakeDisposable fakeDisposable1 = new FakeDisposable();
		FakeDisposable fakeDisposable2 = new FakeDisposable();

		Disposable.Composite<FakeDisposable> composite = new CompositeDisposable<>(fakeDisposable1, fakeDisposable2);

		List<FakeDisposable> content = new ArrayList<>(2);
		composite.forEach(content::add);

		System.out.println(content);

		assertThat(content).containsExactlyInAnyOrder(fakeDisposable1, fakeDisposable2);
	}

}