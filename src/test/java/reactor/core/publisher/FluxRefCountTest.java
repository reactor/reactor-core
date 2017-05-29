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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxRefCountTest {
	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(StreamRefCount.class);

		ctb.addRef("source", Flux.never().publish());
		ctb.addInt("n", 1, Integer.MAX_VALUE);

		ctb.test();
	}*/

	@Test
	public void normal() {
		EmitterProcessor<Integer> e = EmitterProcessor.create();

		Flux<Integer> p = e.publish().refCount();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		e.onNext(1);
		e.onNext(2);

		ts1.cancel();

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		e.onNext(3);

		ts2.cancel();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2, 3)
		.assertNoError()
		.assertNotComplete();
	}

	@Test
	public void normalTwoSubscribers() {
		EmitterProcessor<Integer> e = EmitterProcessor.create();

		Flux<Integer> p = e.publish().refCount(2);

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		e.onNext(1);
		e.onNext(2);

		ts1.cancel();

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		e.onNext(3);

		ts2.cancel();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2, 3)
		.assertNoError()
		.assertNotComplete();
	}

	@Test
	public void upstreamCompletes() {

		Flux<Integer> p = Flux.range(1, 5).publish().refCount();

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

		Flux<Integer> p = Flux.range(1, 5).publish().refCount(2);

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
		Flux<Integer> p = Flux.range(1, 5).publish().refCount(2);

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
	public void reconnectsAfterRefCountZero() {
		AtomicLong subscriptionCount = new AtomicLong();
		AtomicReference<SignalType> termination = new AtomicReference<>();

		Flux<Integer> source = Flux.range(1, 50)
		                           .delayElements(Duration.ofMillis(100))
		                           .doFinally(termination::set)
		                           .doOnSubscribe(s -> subscriptionCount.incrementAndGet());

		Flux<Integer> refCounted = source.publish().refCount(2);

		Disposable sub1 = refCounted.subscribe();
		assertThat(subscriptionCount.get()).isZero();
		assertThat(termination.get()).isNull();

		Disposable sub2 = refCounted.subscribe();
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		sub1.dispose();
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		sub2.dispose();
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isEqualTo(SignalType.CANCEL);

		try {
			sub1 = refCounted.subscribe();
			sub2 = refCounted.subscribe();
			assertThat(subscriptionCount.get()).isEqualTo(2);
		} finally {
			sub1.dispose();
			sub2.dispose();
		}
	}

	@Test
	public void scanMain() {
		ConnectableFlux<Integer> parent = Flux.just(10).publish();
		FluxRefCount<Integer> test = new FluxRefCount<Integer>(parent, 17);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(256);
	}

	@Test
	public void scanInner() {
		Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		FluxRefCount<Integer> main = new FluxRefCount<Integer>(Flux.just(10).publish(), 17);
		FluxRefCount.RefCountMonitor.RefCountInner<Integer> test =
				new FluxRefCount.RefCountMonitor.RefCountInner<Integer>(actual, new FluxRefCount.RefCountMonitor<>(1, main));
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
	}
}
