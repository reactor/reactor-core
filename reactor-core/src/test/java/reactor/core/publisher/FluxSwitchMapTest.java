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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

public class FluxSwitchMapTest {

	@Test
	public void noswitch() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.many().unsafe().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.many().unsafe().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1);

		sp2.emitNext(10);
		sp2.emitNext(20);
		sp2.emitNext(30);
		sp2.emitNext(40);
		sp2.emitComplete();

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitComplete();

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void noswitchBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Sinks.Many<Integer> sp1 = Sinks.many().unsafe().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.many().unsafe().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1);

		sp2.emitNext(10);
		sp2.emitNext(20);
		sp2.emitNext(30);
		sp2.emitNext(40);
		sp2.emitComplete();

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10, 20)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitComplete();

		ts.assertValues(10, 20)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void doswitch() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.many().unsafe().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.many().unsafe().multicast().directBestEffort();
		Sinks.Many<Integer> sp3 = Sinks.many().unsafe().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> v == 1 ? sp2.asFlux() : sp3.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1);

		sp2.emitNext(10);
		sp2.emitNext(20);

		sp1.emitNext(2);

		Assertions.assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();

		sp2.emitNext(30);
		sp3.emitNext(300);
		sp2.emitNext(40);
		sp3.emitNext(400);
		sp2.emitComplete();
		sp3.emitComplete();

		ts.assertValues(10, 20, 300, 400)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitComplete();

		ts.assertValues(10, 20, 300, 400)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void switchRegularQueue() {
		Flux<String> source = Flux.just("a", "bb", "ccc");
		FluxSwitchMap<String, Integer> test = new FluxSwitchMap<>(
				source, s -> Flux.range(1, s.length()),
				ConcurrentLinkedQueue::new, 128);

		StepVerifier.create(test)
		            .expectNext(1, 1, 2, 1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void mainCompletesBefore() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.many().unsafe().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.many().unsafe().multicast().directBestEffort();

		sp1.asFlux().switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1);
		sp1.emitComplete();

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.emitNext(10);
		sp2.emitNext(20);
		sp2.emitNext(30);
		sp2.emitNext(40);
		sp2.emitComplete();

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void mainError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.many().unsafe().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.many().unsafe().multicast().directBestEffort();

		sp1.asFlux().switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1);
		sp1.emitError(new RuntimeException("forced failure"));

		sp2.emitNext(10);
		sp2.emitNext(20);
		sp2.emitNext(30);
		sp2.emitNext(40);
		sp2.emitComplete();

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void innerError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.many().unsafe().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.many().unsafe().multicast().directBestEffort();

		sp1.asFlux().switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1);

		sp2.emitNext(10);
		sp2.emitNext(20);
		sp2.emitNext(30);
		sp2.emitNext(40);
		sp2.emitError(new RuntimeException("forced failure"));

		ts.assertValues(10, 20, 30, 40)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assertions.assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		Assertions.assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
	}

	@Test
	public void mapperThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.many().unsafe().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> {
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		sp1.emitNext(1);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void mapperReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.many().unsafe().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> null)
		   .subscribe(ts);

		sp1.emitNext(1);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void switchOnNextDynamically() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .switchMap(s -> Flux.range(s, 3)))
		            .expectNext(1, 2, 3, 2, 3, 4, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void switchOnNextDynamicallyOnNext() {
		Sinks.Many<Flux<Integer>> up = Sinks.many().unicast().onBackpressureBuffer();
		up.emitNext(Flux.range(1, 3));
		up.emitNext(Flux.range(2, 3).concatWith(Mono.never()));
		up.emitNext(Flux.range(4, 3));
		up.emitComplete();
		StepVerifier.create(Flux.switchOnNext(up.asFlux()))
		            .expectNext(1, 2, 3, 2, 3, 4, 4, 5, 6)
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<String> parent = Flux.just("a", "bb", "ccc");
		FluxSwitchMap<String, Integer> test = new FluxSwitchMap<>(
				parent, s -> Flux.range(1, s.length()),
				ConcurrentLinkedQueue::new, 128);

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMain() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSwitchMap.SwitchMapMain<Integer, Integer> test =
        		new FluxSwitchMap.SwitchMapMain<>(actual, i -> Mono.just(i), Queues.unbounded().get(), 234);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        Assertions.assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(234);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);
        test.queue.add(new FluxSwitchMap.SwitchMapInner<Integer>(test, 1, 0));
        Assertions.assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.error = new IllegalStateException("boom");
        Assertions.assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanInner() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSwitchMap.SwitchMapMain<Integer, Integer> main =
        		new FluxSwitchMap.SwitchMapMain<>(actual, i -> Mono.just(i), Queues.unbounded().get(), 234);
        FluxSwitchMap.SwitchMapInner<Integer> test = new FluxSwitchMap.SwitchMapInner<Integer>(main, 1, 0);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        Assertions.assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
