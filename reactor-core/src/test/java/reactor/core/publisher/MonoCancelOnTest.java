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
package reactor.core.publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoCancelOnTest {

	@Test
	@Timeout(3)
	public void cancelOnDedicatedScheduler() throws Exception {

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Thread> threadHash = new AtomicReference<>(Thread.currentThread());

		Schedulers.single().schedule(() -> threadHash.set(Thread.currentThread()));

		Mono.create(sink -> {
			sink.onDispose(() -> {
				if (threadHash.compareAndSet(Thread.currentThread(), null)) {
					latch.countDown();
				}
			});
		})
		    .cancelOn(Schedulers.single())
		    .subscribeWith(AssertSubscriber.create())
		    .cancel();

		latch.await();
		assertThat(threadHash.get()).isNull();
	}

	@Test
	public void scanOperator() {
		MonoCancelOn<String> test = new MonoCancelOn<>(Mono.empty(), Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}
}
