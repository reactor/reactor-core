/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.blockhound.BlockHound;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.test.util.RaceTestUtils;

@Timeout(5)
public class ReactorBlockHoundIntegrationTest {

	static {
		// Use the builder to load only our integration to avoid false positives
		BlockHound.builder()
		          .with(new ReactorBlockHoundIntegration())
		          .install();
	}

	@Test
	public void shouldDetectBlockingCalls() {
		expectBlockingCall("java.lang.Thread.sleep", future -> {
			Schedulers.parallel()
			          .schedule(() -> {
				          try {
					          Thread.sleep(10);
					          future.complete(null);
				          }
				          catch (Throwable e) {
					          future.completeExceptionally(e);
				          }
			          });
		});
	}

	@Test
	public void shouldDetectBlockingCallsOnSubscribe() {
		expectBlockingCall("java.lang.Thread.yield", future -> {
			Mono.fromRunnable(Thread::yield)
			    .subscribeOn(Schedulers.parallel())
			    .subscribe(future::complete, future::completeExceptionally);
		});
	}

	@Test
	public void shouldDetectBlockingCallsInOperators() {
		expectBlockingCall("java.lang.Thread.yield", future -> {
			Mono.delay(Duration.ofMillis(10))
			    .doOnNext(__ -> Thread.yield())
			    .subscribe(future::complete, future::completeExceptionally);
		});
	}

	@Test
	public void shouldNotReportScheduledFutureTask() {
		for (int i = 0; i < 1_000; i++) {
			Scheduler taskScheduler = Schedulers.newSingle("foo");
			try {
				Runnable dummyRunnable = () -> {
				};

				for (int j = 0; j < 257; j++) {
					taskScheduler.schedule(dummyRunnable, 200, TimeUnit.MILLISECONDS);
				}
				Disposable disposable = taskScheduler.schedule(dummyRunnable, 1, TimeUnit.SECONDS);

				RaceTestUtils.race(disposable::dispose, disposable::dispose);
			}
			finally {
				taskScheduler.dispose();
			}
		}
	}

	void expectBlockingCall(String desc, Consumer<CompletableFuture<Object>> callable) {
		Assertions
				.assertThatThrownBy(() -> {
					CompletableFuture<Object> future = new CompletableFuture<>();
					callable.accept(future);
					future.join();
				})
				.hasMessageContaining("Blocking call! " + desc);
	}
}
