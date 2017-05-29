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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxRefCountGraceTest {

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new IllegalStateException("boom"))
		                        .publish()
				                .refCount(1, Duration.ofMillis(500)))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void upstreamCompletes() {
		Flux<Integer> p = Flux.range(1, 5).publish().refCount(1, Duration.ofMillis(100));

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void upstreamCompletesTwoSubscribers() {
		Flux<Integer> p = Flux.range(1, 5).publish().refCount(2, Duration.ofMillis(100));

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		ts1.assertValueCount(0);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		ts1.assertValues(1, 2, 3, 4, 5);
		ts2.assertValues(1, 2, 3, 4, 5);
	}

	@Test
	public void subscribersComeAndGoBelowThreshold() {
		Flux<Integer> p = Flux.range(1, 5).publish().refCount(2, Duration.ofMillis(500));

		Disposable r = p.subscribe();
		r.dispose();
		p.subscribe().dispose();
		p.subscribe().dispose();
		p.subscribe().dispose();
		p.subscribe().dispose();

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		ts1.assertValueCount(0);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		ts1.assertValues(1, 2, 3, 4, 5);
		ts2.assertValues(1, 2, 3, 4, 5);
	}

	@Test
	public void doesntDisconnectAfterRefCountZeroAndQuickResubscribe()
			throws InterruptedException {
		AtomicLong subscriptionCount = new AtomicLong();
		AtomicReference<SignalType> termination = new AtomicReference<>();

		TestPublisher<Integer> publisher = TestPublisher.create();

		Flux<Integer> source = publisher.flux()
		                                .doFinally(termination::set)
		                                .doOnSubscribe(s -> subscriptionCount.incrementAndGet());

		Flux<Integer> refCounted = source.publish().refCount(2, Duration.ofMillis(800));

		Disposable sub1 = refCounted.subscribe();
		//initial subscribe doesn't reach the count
		assertThat(subscriptionCount.get()).isZero();
		assertThat(termination.get()).isNull();

		Disposable sub2 = refCounted.subscribe();
		//second subscribe does reaches the count
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		sub1.dispose();
		//all subscribers are not disposed so source isn't terminated
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		sub2.dispose();
		//all subscribers are now disposed, but grace period kicks in
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		//we re-subscribe quickly before grace period elapses
		sub1 = refCounted.subscribe();
		sub2 = refCounted.subscribe();

		//we sleep until sure the initial grace period is over
		Thread.sleep(500);

		//we expect the subscription to be still active
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		publisher.complete();
		assertThat(termination.get()).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void doesDisconnectAfterRefCountZeroAndSlowResubscribe()
			throws InterruptedException {
		AtomicLong subscriptionCount = new AtomicLong();
		AtomicReference<SignalType> termination = new AtomicReference<>();

		TestPublisher<Integer> publisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		Flux<Integer> source = publisher.flux()
		                                .doFinally(termination::set)
		                                .doOnSubscribe(s -> subscriptionCount.incrementAndGet());

		Flux<Integer> refCounted = source.publish().refCount(2, Duration.ofMillis(500));

		Disposable sub1 = refCounted.subscribe();
		//initial subscribe doesn't reach the count
		assertThat(subscriptionCount.get()).isZero();
		assertThat(termination.get()).isNull();

		Disposable sub2 = refCounted.subscribe();
		//second subscribe does reaches the count
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		sub1.dispose();
		//all subscribers are not disposed so source isn't terminated
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		sub2.dispose();
		//all subscribers are now disposed, but grace period kicks in
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		//we wait for grace period to elapse
		Thread.sleep(600);
		assertThat(termination.get()).isEqualTo(SignalType.CANCEL);
		termination.set(null);

		//we then resubscribe
		sub1 = refCounted.subscribe();
		sub2 = refCounted.subscribe();
		assertThat(subscriptionCount.get()).isEqualTo(2);

		//since the TestPublisher doesn't cleanup on termination, we can re-evaluate the doFinally
		publisher.complete();
		assertThat(termination.get()).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void scanMain() {
		ConnectableFlux<Integer> parent = Flux.just(10).publish();
		FluxRefCountGrace<Integer> test = new FluxRefCountGrace<Integer>(parent, 17, Duration.ofSeconds(1), Schedulers.single());

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(256);
	}

}
