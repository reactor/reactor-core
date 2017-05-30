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

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDematerializeTest extends FluxOperatorTest<Signal<String>, String> {

	@Override
	@SuppressWarnings("unchecked")
	protected List<Scenario<Signal<String>, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(Flux::<String>dematerialize)
					.producer(1, i -> Signal.complete())
					.receiverEmpty(),

				scenario(Flux::<String>dematerialize)
						.producer(1, i -> Signal.subscribe(Operators.emptySubscription()))
						.receiverEmpty(),

				scenario(Flux::<String>dematerialize)
						.receive(s -> assertThat(s).isEqualTo(item(0).get()),
								s -> assertThat(s).isEqualTo(item(1).get()),
								s -> assertThat(s).isEqualTo(item(2).get()))
						.verifier(step -> step.expectNext("test")
						                      .expectNext("test1")
						                      .consumeSubscriptionWith(s -> {
							                      if(s instanceof FluxDematerialize.DematerializeSubscriber) {
								                      FluxDematerialize.DematerializeSubscriber m =
										                      (FluxDematerialize.DematerializeSubscriber) s;

								                      m.peek();
								                      m.poll();
								                      m.size();
							                      }
						                      })
						                      .expectNext("test2")
						                      .consumeSubscriptionWith(s -> {
							                      if(s instanceof FluxDematerialize.DematerializeSubscriber){
								                      FluxDematerialize.DematerializeSubscriber m =
										                      (FluxDematerialize.DematerializeSubscriber)s;

								                      m.peek();
								                      m.poll();
								                      m.size();

								                      try{
									                      m.offer(null);
									                      Assert.fail();
								                      }
								                      catch (UnsupportedOperationException u){
									                      //ignore
								                      }

								                      try{
									                      m.iterator();
									                      Assert.fail();
								                      }
								                      catch (UnsupportedOperationException u){
									                      //ignore
								                      }
							                      }
						                      })
						                      .verifyComplete())
		);
	}

	@Override
	protected List<Scenario<Signal<String>, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(Flux::<String>dematerialize)
						.producer(1, i -> Signal.error(exception())));
	}

	@Override
	protected List<Scenario<Signal<String>, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(Flux::<String>dematerialize)
		);
	}

	@Override
	protected Scenario<Signal<String>, String> defaultScenarioOptions(Scenario<Signal<String>, String> defaultOptions) {
		return defaultOptions.producer(3, i -> i == 0 ?
				Signal.next("test") :
				Signal.next("test"+i)
		).droppedItem(Signal.next("dropped"));
	}

	Signal<Integer> error = Signal.error(new RuntimeException("Forced failure"));

	@Test
	public void singleCompletion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> dematerialize = Flux.just(Signal.<Integer>complete()).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void singleError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> dematerialize = Flux.just(error)
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertNotComplete();
	}

	@Test
	public void immediateCompletion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(Signal.<Integer>complete()).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void immediateError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(error).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertNotComplete();
	}

	@Test
	public void completeAfterSingleSignal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.<Integer>complete()).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorAfterSingleSignal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(Signal.next(1), error).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertNotComplete();
	}

	@Test
	public void twoSignalsAndComplete() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2), Signal.<Integer>complete()).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void twoSignalsAndError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2), error).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertNotComplete();
	}

	@Test
	public void neverEnding() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2),
				Signal.next(3), Signal.<Integer>complete())
		                                  .concatWith(Flux.never())
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void dematerialize() {
		StepVerifier.create(Flux.just(Signal.next("Three"),
				Signal.next("Two"),
				Signal.next("One"),
				Signal.complete())
		                        .dematerialize())
		            .expectNext("Three")
		            .expectNext("Two")
		            .expectNext("One")
		            .verifyComplete();
	}

	@Test
	public void scanSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null,
				sub -> sub.request(100));
		FluxDematerialize.DematerializeSubscriber<String> test =
				new FluxDematerialize.DematerializeSubscriber<>(actual);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(0);
		test.value = "foo";
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();


		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}