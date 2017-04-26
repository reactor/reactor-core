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

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoToCallableTest {

	@Test
	public void testDelayed() throws Exception {
		Callable<String> callable = Mono.just("foo")
		                                .delayElement(Duration.ofMillis(500))
		                                .toCallable();

		long start = System.nanoTime();
		assertThat(callable.call())
				.isEqualTo("foo");
		assertThat(System.nanoTime() - start)
				.overridingErrorMessage("Call took less than 500ms")
				.isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(500));
	}

	@Test
	public void testDelayedNotSubscribedIfNotCalled() {
		AtomicBoolean subscribed = new AtomicBoolean();

		Mono<String> source = Mono.just("foo")
		                          .delayElement(Duration.ofMillis(500))
		                          .doOnSubscribe(s -> subscribed.set(true));

		Callable<String> callable = source.toCallable();

		assertThat(subscribed.get()).isFalse();
	}

	@Test
	public void testError() {
		Callable<String> callable = Mono.<String>error(new IllegalArgumentException("boom"))
				.toCallable();

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(callable::call)
	            .withMessage("boom");
	}

	@Test
	public void testEmpty() throws Exception {
		Callable<String> callable = Mono.<String>empty()
				.toCallable();

		assertThat(callable.call()).isNull();
	}

	@Test
	public void multipleCallSubscribesOnce() throws Exception {
		AtomicLong subscribed = new AtomicLong();

		Mono<String> source = Mono.just("foo")
		                          .doOnSubscribe(s -> subscribed.incrementAndGet());

		Callable<String> callable = source.toCallable();
		callable.call();
		callable.call();
		callable.call();
		callable.call();

		assertThat(subscribed.get()).isEqualTo(1);
	}
}