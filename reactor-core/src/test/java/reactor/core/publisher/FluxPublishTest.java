/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxPublishTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.prefetch(Queues.SMALL_BUFFER_SIZE);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.publish().autoConnect()),

				scenario(f -> f.publish().refCount())
		);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failPrefetch(){
		Flux.never()
		    .publish( -1);
	}

	@Test
	public void prematureOnComplete() {
		EmitterProcessor<Flux<String>> incomingProcessor = EmitterProcessor.create(false);

		Flux.just("ALPHA", "BRAVO", "CHARLIE", "DELTA", "ALPHA", "BRAVO", "CHARLIE", "DELTA", "ALPHA", "BRAVO", "CHARLIE", "DELTA")
		    .log("stream.incoming")
		    .windowWhile(s -> !"DELTA".equals(s),1 )
		    .subscribe(incomingProcessor);

		AtomicInteger windowIndex = new AtomicInteger(0);
		AtomicInteger nextIndex = new AtomicInteger(0);

		System.out.println("ZERO");
		incomingProcessor
				.next()
				.flatMapMany(flux -> flux
						.takeWhile(s -> !"CHARLIE".equals(s))
						.log(String.format("stream.window.%d", windowIndex.getAndIncrement())))
				.log(String.format("stream.next.%d", nextIndex.getAndIncrement()))
				.as(StepVerifier::create)
				.expectNextCount(2)
				.verifyComplete();

		System.out.println("ONE");
		incomingProcessor.next()
		                 .flatMapMany(flux -> flux
				                 .takeWhile(s -> !"CHARLIE".equals(s))
				                 .log(String.format("stream.window.%d", windowIndex.getAndIncrement())))
		                 .log(String.format("stream.next.%d", nextIndex.getAndIncrement()))
		                 .as(StepVerifier::create)
		                 .expectNextCount(2)
		                 .verifyComplete();

		System.out.println("TWO");
		incomingProcessor.next()
		                 .flatMapMany(flux -> flux
				                 .takeWhile(s -> !"CHARLIE".equals(s))
				                 .log(String.format("stream.window.%d", windowIndex.getAndIncrement())))
		                 .log(String.format("stream.next.%d", nextIndex.getAndIncrement()))
		                 .as(StepVerifier::create)
		                 .expectNextCount(2)
		                 .verifyComplete();
	}

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(StreamPublish.class);

		ctb.addRef("source", Flux.never());
		ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
		ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());

		ctb.test();
	}*/

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).hide().publish();

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		ConnectableFlux<Integer> p = Flux.range(1, 5).hide().publish();

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalAsyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();

		UnicastProcessor<Integer> up = UnicastProcessor.create(Queues.<Integer>get(8).get());
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		ConnectableFlux<Integer> p = up.publish();

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalBackpressuredAsyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		UnicastProcessor<Integer> up = UnicastProcessor.create(Queues.<Integer>get(8).get());
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		ConnectableFlux<Integer> p = up.publish();

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalSyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).publish(5);

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalBackpressuredSyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		ConnectableFlux<Integer> p = Flux.range(1, 5).publish(5);

		p.subscribe(ts1);
		p.subscribe(ts2);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/1302
	@Test
	public void boundaryFused() {
		Flux.range(1, 10000)
		    .publishOn(Schedulers.single())
		    .map(v -> Thread.currentThread().getName().contains("single-") ? "single" : ("BAD-" + v + Thread.currentThread().getName()))
		    .share()
		    .publishOn(Schedulers.elastic())
		    .distinct()
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectNext("single")
		    .expectComplete()
		    .verify(Duration.ofSeconds(5));
	}

	@Test
	public void disconnect() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		EmitterProcessor<Integer> e = EmitterProcessor.create();

		ConnectableFlux<Integer> p = e.publish();

		p.subscribe(ts);

		Disposable r = p.connect();

		e.onNext(1);
		e.onNext(2);

		r.dispose();

		ts.assertValues(1, 2)
		.assertError(CancellationException.class)
		.assertNotComplete();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
	}

	@Test
	public void disconnectBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		EmitterProcessor<Integer> e = EmitterProcessor.create();

		ConnectableFlux<Integer> p = e.publish();

		p.subscribe(ts);

		Disposable r = p.connect();

		r.dispose();

		ts.assertNoValues()
		.assertError(CancellationException.class)
		.assertNotComplete();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		EmitterProcessor<Integer> e = EmitterProcessor.create();

		ConnectableFlux<Integer> p = e.publish();

		p.subscribe(ts);

		p.connect();

		e.onNext(1);
		e.onNext(2);
		e.onError(new RuntimeException("forced failure"));

		ts.assertValues(1, 2)
		.assertError(RuntimeException.class)
		  .assertErrorWith( x -> Assert.assertTrue(x.getMessage().contains("forced failure")))
		.assertNotComplete();
	}

	@Test
	public void fusedMapInvalid() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).map(v -> (Integer)null).publish();

		p.subscribe(ts);

		p.connect();

		ts.assertNoValues()
		.assertError(NullPointerException.class)
		.assertNotComplete();
	}


	@Test
	public void retry() {
		DirectProcessor<Integer> dp = DirectProcessor.create();
		StepVerifier.create(
				dp.publish()
				  .autoConnect().<Integer>handle((s1, sink) -> {
					if (s1 == 1) {
						sink.error(new RuntimeException());
					}
					else {
						sink.next(s1);
					}
				}).retry())
		            .then(() -> {
			            dp.onNext(1);
			            dp.onNext(2);
			            dp.onNext(3);
		            })
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();

		// Need to explicitly complete processor due to use of publish()
		dp.onComplete();
	}

	@Test
	public void retryWithPublishOn() {
		DirectProcessor<Integer> dp = DirectProcessor.create();
		StepVerifier.create(
				dp.publishOn(Schedulers.parallel()).publish()
				  .autoConnect().<Integer>handle((s1, sink) -> {
					if (s1 == 1) {
						sink.error(new RuntimeException());
					}
					else {
						sink.next(s1);
					}
				}).retry())
		            .then(() -> {
			            dp.onNext(1);
			            dp.onNext(2);
			            dp.onNext(3);
		            })
		            .expectNext(2, 3)
		            .thenCancel()
		            .verify();

		// Need to explicitly complete processor due to use of publish()
		dp.onComplete();
	}

	@Test
    public void scanMain() {
        Flux<Integer> parent = Flux.just(1).map(i -> i);
        FluxPublish<Integer> test = new FluxPublish<>(parent, 123, Queues.unbounded());

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
    }

	@Test
    public void scanSubscriber() {
        FluxPublish<Integer> main = new FluxPublish<>(Flux.just(1), 123, Queues.unbounded());
        FluxPublish.PublishSubscriber<Integer> test = new FluxPublish.PublishSubscriber<>(789, main);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(789);
        test.queue.add(5);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        test.error = new IllegalArgumentException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        test = new FluxPublish.PublishSubscriber<>(789, main);
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.onSubscribe(Operators.cancelledSubscription());
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanInner() {
		FluxPublish<Integer> main = new FluxPublish<>(Flux.just(1), 123, Queues.unbounded());
        FluxPublish.PublishSubscriber<Integer> parent = new FluxPublish.PublishSubscriber<>(789, main);
        Subscription sub = Operators.emptySubscription();
        parent.onSubscribe(sub);
        FluxPublish.PublishInner<Integer> test = new FluxPublish.PublishInner<>(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(parent);
        test.parent = parent;
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        test.request(35);
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        parent.terminate();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanPubSubInner() {
		FluxPublish<Integer> main = new FluxPublish<>(Flux.just(1), 123, Queues.unbounded());
        FluxPublish.PublishSubscriber<Integer> parent = new FluxPublish.PublishSubscriber<>(789, main);
        Subscription sub = Operators.emptySubscription();
        parent.onSubscribe(sub);
        FluxPublish.PubSubInner<Integer> test = new FluxPublish.PublishInner<>(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(parent);
        test.request(35);
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    //see https://github.com/reactor/reactor-core/issues/1290
    @Test
    public void syncFusionSingle() { //single value in the SYNC fusion
	    final ConnectableFlux<String> publish = Flux.just("foo")
	                                 .publish();

		StepVerifier.create(publish)
		            .then(publish::connect)
		            .expectNext("foo")
		            .expectComplete()
		            .verify(Duration.ofSeconds(4));
    }

    //see https://github.com/reactor/reactor-core/issues/1290
	@Test
	public void syncFusionMultiple() { //multiple values in the SYNC fusion
		final ConnectableFlux<Integer> publish = Flux.range(1, 5)
		                                             .publish();

		StepVerifier.create(publish)
		            .then(publish::connect)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify(Duration.ofSeconds(4));
	}

	//see https://github.com/reactor/reactor-core/issues/1528
	@Test(timeout = 4000)
	public void syncFusionFromInfiniteStream() {
		final ConnectableFlux<Integer> publish =
				Flux.fromStream(Stream.iterate(0, i -> i + 1))
				    .publish();

		StepVerifier.create(publish)
		            .then(publish::connect)
		            .thenConsumeWhile(i -> i < 10)
		            .expectNextCount(10)
		            .thenCancel()
		            .verify(Duration.ofSeconds(4));
	}

	//see https://github.com/reactor/reactor-core/issues/1528
	@Test(timeout = 4000)
	public void syncFusionFromInfiniteStreamAndTake() {
		final Flux<Integer> publish =
				Flux.fromStream(Stream.iterate(0, i -> i + 1))
				    .publish()
				    .autoConnect()
				    .take(10);

		StepVerifier.create(publish)
		            .expectNextCount(10)
		            .expectComplete()
		            .verify(Duration.ofSeconds(4));
	}

	@Test
	public void dataDroppedIfConnectImmediately() {
		TestPublisher<Integer> publisher = TestPublisher.create();
		ConnectableFlux<Integer> connectableFlux = publisher.flux().publish();

		connectableFlux.connect();

		publisher.next(1);
		publisher.next(2);
		publisher.next(3);

		StepVerifier.create(connectableFlux)
		            .expectSubscription()
		            .then(() -> publisher.next(99))
		            .expectNext(99)
		            .then(publisher::complete)
		            .verifyComplete();
	}

	@Test
	public void dataDroppedIfAutoconnectZero() {
		TestPublisher<Integer> publisher = TestPublisher.create();
		Flux<Integer> flux = publisher.flux().publish().autoConnect(0);

		publisher.next(1);
		publisher.next(2);
		publisher.next(3);

		StepVerifier.create(flux)
		            .expectSubscription()
		            .then(() -> publisher.next(99))
		            .expectNext(99)
		            .then(publisher::complete)
		            .verifyComplete();
	}

	@Test
	public void removeUnknownInnerIgnored() {
		FluxPublish.PublishSubscriber<Integer> subscriber = new FluxPublish.PublishSubscriber<>(1, null);
		FluxPublish.PublishInner<Integer> inner = new FluxPublish.PublishInner<>(null);
		FluxPublish.PublishInner<Integer> notInner = new FluxPublish.PublishInner<>(null);

		subscriber.add(inner);
		assertThat(subscriber.subscribers).as("adding inner").hasSize(1);

		subscriber.remove(notInner);
		assertThat(subscriber.subscribers).as("post remove notInner").hasSize(1);

		subscriber.remove(inner);
		assertThat(subscriber.subscribers).as("post remove inner").isEmpty();
	}

	@Test
	public void subscriberContextPropagation() {
		String key = "key";
		int expectedValue = 1;

		AtomicReference<Context> reference = new AtomicReference<>();

		Flux<Integer> integerFlux =
				Flux.just(1, 2, 3)
				    .flatMap(value ->
						    Mono.subscriberContext()
						        .doOnNext(reference::set)
						        .thenReturn(value)
				    )
				    .publish()
				    .autoConnect(2);

		integerFlux.subscriberContext(Context.of(key, expectedValue))
		           .subscribe();

		integerFlux.subscriberContext(Context.of(key, 2))
		           .subscribe();

		assertThat((int) reference.get().get(key)).isEqualTo(expectedValue);
	}
}
