/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxCancelOnTest {

	@Test(timeout = 3000L)
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
		Assert.assertNull(threadHash.get());
	}


	@Test
	public void scanSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, null, null, null);
		FluxCancelOn.CancelSubscriber<String> test = new FluxCancelOn.CancelSubscriber<>(actual, Schedulers.single());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}