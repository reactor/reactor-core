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

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.concurrent.WaitStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static reactor.core.Scannable.Attr.TERMINATED;
import static reactor.core.Scannable.Attr.PARENT;
import static reactor.core.Scannable.Attr.ERROR;

@SuppressWarnings("deprecation")
public class EventLoopProcessorTest {

	EventLoopProcessor<String> test;

	@Before
	public void initProcessor() {
		test = initProcessor(Executors.newSingleThreadExecutor());
	}

	private static EventLoopProcessor<String> initProcessor(ExecutorService executor) {
		return new EventLoopProcessor<String>(128,
				Thread::new,
				executor,
				Executors.newSingleThreadExecutor(),
				true,
				false,
				() -> null,
				WaitStrategy.sleeping()) {
			@Override
			public void run() {

			}

			@Override
			public long getPending() {
				return 456;
			}

			@Override
			public void subscribe(CoreSubscriber<? super String> actual) {

			}

			@Override
			void doError(Throwable throwable) {
				this.error = throwable;
			}
		};
	}

	@Test
	public void scanMain() {
		assertThat(test.scan(PARENT)).isNull();
		assertThat(test.scan(TERMINATED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(TERMINATED)).isTrue();
		assertThat(test.scan(ERROR)).hasMessage("boom");

		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(128);
	}

	Condition<ExecutorService> shutdown = new Condition<>(ExecutorService::isShutdown, "is shutdown");
	Condition<ExecutorService> terminated = new Condition<>(ExecutorService::isTerminated, "is terminated");

	@Test
	public void awaitTerminationImmediate() {
		assertThat(test.executor).isNotNull();
		test.executor.submit(() -> {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		boolean shutResult = test.awaitAndShutdown(Duration.ofMillis(-1));
		assertThat(test.executor)
				.isNotNull()
				.is(shutdown)
				.isNot(terminated);

		assertThat(shutResult).isFalse();
	}

	@Test
	public void awaitTerminationNanosDuration() {
		assertThat(test.executor).isNotNull();
		test.executor.submit(() -> {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		boolean shutResult = test.awaitAndShutdown(Duration.ofNanos(1000));
		assertThat(test.executor)
				.isNotNull()
				.is(shutdown)
				.isNot(terminated);

		assertThat(shutResult).isFalse();
	}

	@Test
	public void awaitTerminationNanosLong() {
		assertThat(test.executor).isNotNull();
		test.executor.submit(() -> {
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		@SuppressWarnings("deprecation")
		boolean shutResult = test.awaitAndShutdown(1000, TimeUnit.NANOSECONDS);
		assertThat(test.executor)
				.isNotNull()
				.is(shutdown)
				.isNot(terminated);

		assertThat(shutResult).isFalse();
	}

	@Test
	public void awaitAndShutdownInterrupt() throws InterruptedException {
		ExecutorService executor = Mockito.mock(ExecutorService.class);
		doThrow(new InterruptedException("boom"))
				.when(executor).awaitTermination(anyLong(), any());
		EventLoopProcessor<String> interruptingProcessor = initProcessor(executor);

		boolean result = interruptingProcessor.awaitAndShutdown(Duration.ofMillis(100));

		assertThat(Thread.currentThread().isInterrupted()).as("interrupted").isTrue();
		assertThat(result).as("await failed").isFalse();
	}

	@Test
	public void awaitAndShutdownLongInterrupt() throws InterruptedException {
		ExecutorService executor = Mockito.mock(ExecutorService.class);
		doThrow(new InterruptedException("boom"))
				.when(executor).awaitTermination(anyLong(), any());
		EventLoopProcessor<String> interruptingProcessor = initProcessor(executor);

		@SuppressWarnings("deprecation")
		boolean result = interruptingProcessor.awaitAndShutdown(100, TimeUnit.MILLISECONDS);

		assertThat(Thread.currentThread().isInterrupted()).as("interrupted").isTrue();
		assertThat(result).as("await failed").isFalse();
	}

}