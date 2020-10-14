/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class BlockingOptionalMonoSubscriberTest {

	@Test
	public void optionalValued() {
		Optional<String> result = Mono.just("foo").blockOptional();

		assertThat(result).contains("foo");
	}

	@Test
	public void optionalValuedDelayed() {
		Optional<String> result = Mono.just("foo")
		                              .delayElement(Duration.ofMillis(500))
		                              .blockOptional();

		assertThat(result).contains("foo");
	}

	@Test
	public void optionalEmpty() {
		Optional<String> result = Mono.<String>empty().blockOptional();

		assertThat(result).isEmpty();
	}

	@Test
	public void optionalEmptyDelayed() {
		Optional<String> result = Mono.<String>empty()
				.delayElement(Duration.ofMillis(500))
				.blockOptional();

		assertThat(result).isEmpty();
	}

	@Test
	public void optionalError() {
		Mono<String> source = Mono.error(new IllegalStateException("boom"));
		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(source::blockOptional)
				.withMessage("boom");
	}

	@Test
	public void timeoutOptionalValued() {
		Optional<String> result = Mono.just("foo")
		                              .blockOptional(Duration.ofMillis(500));

		assertThat(result).contains("foo");
	}

	@Test
	public void timeoutOptionalEmpty() {
		Optional<String> result = Mono.<String>empty()
		                              .blockOptional(Duration.ofMillis(500));

		assertThat(result).isEmpty();
	}

	@Test
	public void timeoutOptionalError() {
		Mono<String> source = Mono.error(new IllegalStateException("boom"));

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> source.blockOptional(Duration.ofMillis(500)))
				.withMessage("boom");
	}

	@Test
	public void timeoutOptionalTimingOut() {
		Mono<Long> source = Mono.delay(Duration.ofMillis(500));

		// Using sub-millis timeouts after gh-1734
		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> source.blockOptional(Duration.ofNanos(100)))
				.withMessage("Timeout on blocking read for 100 NANOSECONDS");
	}

	@Test
	public void isDisposedBecauseCancelled() {
		BlockingOptionalMonoSubscriber<String> test = new BlockingOptionalMonoSubscriber<>();

		assertThat(test.isDisposed()).isFalse();

		test.dispose();

		assertThat(test.isDisposed()).isTrue();
	}

	@Test
	public void isDisposedBecauseValued() {
		BlockingOptionalMonoSubscriber<String> test = new BlockingOptionalMonoSubscriber<>();

		assertThat(test.isDisposed()).isFalse();

		test.onNext("foo");

		assertThat(test.isDisposed()).isTrue();
	}

	@Test
	public void isDisposedBecauseComplete() {
		BlockingOptionalMonoSubscriber<String> test = new BlockingOptionalMonoSubscriber<>();

		assertThat(test.isDisposed()).isFalse();

		test.onComplete();

		assertThat(test.isDisposed()).isTrue();
	}

	@Test
	public void isDisposedBecauseError() {
		BlockingOptionalMonoSubscriber<String> test = new BlockingOptionalMonoSubscriber<>();

		assertThat(test.isDisposed()).isFalse();

		test.onError(new IllegalArgumentException("boom"));

		assertThat(test.isDisposed()).isTrue();
	}

	@Test
	public void scanOperator() {
		BlockingOptionalMonoSubscriber<String> test = new BlockingOptionalMonoSubscriber<>();

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.NAME)).as("NAME (not covered)").isNull();

		assertThat(test.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).as("PREFETCH").isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).as("TERMINATED").isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).as("CANCELLED").isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).as("ERROR").isNull();

		test.onError(new IllegalArgumentException());

		assertThat(test.scan(Scannable.Attr.ERROR)).as("ERROR after onError").isInstanceOf(IllegalArgumentException.class);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).as("TERMINATED after onError").isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).as("CANCELLED after onError").isFalse();

		test.dispose();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).as("CANCELLED after dispose()").isTrue();
	}

	@Test
	public void interruptBlock() throws InterruptedException {
		BlockingOptionalMonoSubscriber<String> test = new BlockingOptionalMonoSubscriber<>();
		AtomicReference<Throwable> errorHandler = new AtomicReference<>();

		Thread t = new Thread(test::blockingGet);
		t.setUncaughtExceptionHandler((t1, e) -> errorHandler.set(e));
		t.start();
		t.interrupt();
		t.join();

		assertThat(test.isDisposed()).as("interrupt disposes").isTrue();
		assertThat(errorHandler.get())
				.isNotNull()
				.isInstanceOf(RuntimeException.class)
				.hasCauseInstanceOf(InterruptedException.class)
				.hasSuppressedException(new Exception("#blockOptional() has been interrupted"));
	}

	@Test
	public void interruptBlockTimeout() throws InterruptedException {
		BlockingOptionalMonoSubscriber<String> test = new BlockingOptionalMonoSubscriber<>();
		AtomicReference<Throwable> errorHandler = new AtomicReference<>();

		Thread t = new Thread(() -> test.blockingGet(2, TimeUnit.SECONDS));
		t.setUncaughtExceptionHandler((t1, e) -> errorHandler.set(e));
		t.start();
		t.interrupt();
		t.join();

		assertThat(test.isDisposed()).as("interrupt disposes").isTrue();
		assertThat(errorHandler.get())
				.isNotNull()
				.isInstanceOf(RuntimeException.class)
				.hasCauseInstanceOf(InterruptedException.class)
				.hasSuppressedException(new Exception("#blockOptional(timeout) has been interrupted"));
	}
}
