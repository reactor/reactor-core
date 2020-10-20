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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.*;

/**
 * @author David Karnok
 */
public class SwapDisposableTest {

	private Disposables.SwapDisposable sequentialDisposable;

	@BeforeEach
	public void setUp() {
		sequentialDisposable = new Disposables.SwapDisposable();
	}

	@Test
	public void unsubscribingWithoutUnderlyingDoesNothing() {
		sequentialDisposable.dispose();
	}

	@Test
	public void getDisposableShouldReturnset() {
		final Disposable underlying = mock(Disposable.class);
		sequentialDisposable.update(underlying);
		assertThat(sequentialDisposable.get()).isSameAs(underlying);

		final Disposable another = mock(Disposable.class);
		sequentialDisposable.update(another);
		assertThat(sequentialDisposable.get()).isSameAs(another);
	}

	@Test
	public void notDisposedWhenReplaced() {
		final Disposable underlying = mock(Disposable.class);
		sequentialDisposable.update(underlying);

		sequentialDisposable.replace(() -> {});
		sequentialDisposable.dispose();

		verify(underlying, never()).dispose();
	}

	@Test
	public void unsubscribingTwiceDoesUnsubscribeOnce() {
		Disposable underlying = mock(Disposable.class);
		sequentialDisposable.update(underlying);

		sequentialDisposable.dispose();
		verify(underlying).dispose();

		sequentialDisposable.dispose();
		verifyNoMoreInteractions(underlying);
	}

	@Test
	public void settingSameDisposableTwiceDoesUnsubscribeIt() {
		Disposable underlying = mock(Disposable.class);
		sequentialDisposable.update(underlying);
		verifyZeroInteractions(underlying);
		sequentialDisposable.update(underlying);
		verify(underlying).dispose();
	}

	@Test
	public void unsubscribingWithSingleUnderlyingUnsubscribes() {
		Disposable underlying = mock(Disposable.class);
		sequentialDisposable.update(underlying);
		underlying.dispose();
		verify(underlying).dispose();
	}

	@Test
	public void replacingFirstUnderlyingCausesUnsubscription() {
		Disposable first = mock(Disposable.class);
		sequentialDisposable.update(first);
		Disposable second = mock(Disposable.class);
		sequentialDisposable.update(second);
		verify(first).dispose();
	}

	@Test
	public void whenUnsubscribingSecondUnderlyingUnsubscribed() {
		Disposable first = mock(Disposable.class);
		sequentialDisposable.update(first);
		Disposable second = mock(Disposable.class);
		sequentialDisposable.update(second);
		sequentialDisposable.dispose();
		verify(second).dispose();
	}

	@Test
	public void settingUnderlyingWhenUnsubscribedCausesImmediateUnsubscription() {
		sequentialDisposable.dispose();
		Disposable underlying = mock(Disposable.class);
		sequentialDisposable.update(underlying);
		verify(underlying).dispose();
	}

	@Test
	@Timeout(1)
	public void settingUnderlyingWhenUnsubscribedCausesImmediateUnsubscriptionConcurrently()
			throws InterruptedException {
		final Disposable firstSet = mock(Disposable.class);
		sequentialDisposable.update(firstSet);

		final CountDownLatch start = new CountDownLatch(1);

		final int count = 10;
		final CountDownLatch end = new CountDownLatch(count);

		final List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			final Thread t = new Thread(() -> {
				try {
					start.await();
					sequentialDisposable.dispose();
				} catch (InterruptedException e) {
					fail(e.getMessage());
				} finally {
					end.countDown();
				}
			});
			t.start();
			threads.add(t);
		}

		final Disposable underlying = mock(Disposable.class);
		start.countDown();
		sequentialDisposable.update(underlying);
		end.await();
		verify(firstSet).dispose();
		verify(underlying).dispose();

		for (final Thread t : threads) {
			t.join();
		}
	}

	@Test
	public void concurrentSetDisposableShouldNotInterleave()
			throws InterruptedException {
		final int count = 10;
		final List<Disposable> subscriptions = new ArrayList<>();

		final CountDownLatch start = new CountDownLatch(1);
		final CountDownLatch end = new CountDownLatch(count);

		final List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			final Disposable subscription = mock(Disposable.class);
			subscriptions.add(subscription);

			final Thread t = new Thread(() -> {
				try {
					start.await();
					sequentialDisposable.update(subscription);
				} catch (InterruptedException e) {
					fail(e.getMessage());
				} finally {
					end.countDown();
				}
			});
			t.start();
			threads.add(t);
		}

		start.countDown();
		end.await();
		sequentialDisposable.dispose();

		for (final Disposable subscription : subscriptions) {
			verify(subscription).dispose();
		}

		for (final Thread t : threads) {
			t.join();
		}
	}
}
