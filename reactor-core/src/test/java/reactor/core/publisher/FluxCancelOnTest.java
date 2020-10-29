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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.from;

public class FluxCancelOnTest {

	@Test
	@Timeout(3)
	public void cancelOnDedicatedScheduler() throws Exception {

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Thread> threadHash = new AtomicReference<>(Thread.currentThread());

		Schedulers.single().schedule(() -> threadHash.set(Thread.currentThread()));

		Flux.create(sink -> {
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
		assertThat(threadHash).hasValue(null);
	}

	@Test
	public void scanOperator() {
		Scheduler scheduler = Schedulers.boundedElastic();
		final Flux<Integer> flux = Flux.just(1).cancelOn(scheduler);

		assertThat(flux).isInstanceOf(Scannable.class);
		assertThat(from(flux).scan(Scannable.Attr.RUN_ON)).isSameAs(scheduler);
		assertThat(from(flux).scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, null, null, null);
		Scheduler scheduler = Schedulers.single();
		FluxCancelOn.CancelSubscriber<String> test = new FluxCancelOn.CancelSubscriber<>(actual, scheduler);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(scheduler);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}
