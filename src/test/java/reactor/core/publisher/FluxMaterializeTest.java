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

public class FluxMaterializeTest
		extends FluxOperatorTest<String, Signal<String>> {

	@Override
	@SuppressWarnings("unchecked")
	protected List<Scenario<String, Signal<String>>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(Flux::materialize)
						.receive(s -> assertThat(s).isEqualTo(Signal.next(item(0))),
								s -> assertThat(s).isEqualTo(Signal.next(item(1))),
								s -> assertThat(s).isEqualTo(Signal.next(item(2))),
								s -> assertThat(s).isEqualTo(Signal.complete()))
						.verifier(step -> step.expectNext(Signal.next(item(0)))
						                      .expectNext(Signal.next(item(1)))
						                      .consumeSubscriptionWith(s -> {
							                      if(s instanceof FluxMaterialize.MaterializeSubscriber) {
								                      FluxMaterialize.MaterializeSubscriber m =
										                      (FluxMaterialize.MaterializeSubscriber) s;

								                      m.peek();
								                      m.poll();
								                      m.size();
							                      }
						                      })
						                      .expectNext(Signal.next(item(2)))
						                      .thenRequest(1)
						                      .expectNext(Signal.complete())
						                      .consumeSubscriptionWith(s -> {
							                      if(s instanceof FluxMaterialize.MaterializeSubscriber){
								                      FluxMaterialize.MaterializeSubscriber m =
										                      (FluxMaterialize.MaterializeSubscriber)s;

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
	protected List<Scenario<String, Signal<String>>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(Flux::materialize)
						.verifier(step -> {
							Hooks.onErrorDropped(c -> assertThat(c).hasMessage("dropped"));
							Hooks.onNextDropped(c -> assertThat(c).isEqualTo("dropped"));
							step.assertNext(s -> assertThat(s
									.getThrowable()).hasMessage("test"))
							    .verifyComplete();
						})
		);
	}

	@Test
	public void completeOnlyBackpressured() {
		AssertSubscriber<Signal<Integer>> ts = AssertSubscriber.create(0L);

		Flux.<Integer>empty().materialize()
		                     .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Signal.complete())
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorOnlyBackpressured() {
		AssertSubscriber<Signal<Integer>> ts = AssertSubscriber.create(0L);

		RuntimeException ex = new RuntimeException();

		Flux.<Integer>error(ex).materialize()
		                       .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Signal.error(ex))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void materialize() {
		StepVerifier.create(Flux.just("Three", "Two", "One")
		                        .materialize())
		            .expectNextMatches(s -> s.isOnNext() && s.get()
		                                                     .equals("Three"))
		            .expectNextMatches(s -> s.isOnNext() && s.get()
		                                                     .equals("Two"))
		            .expectNextMatches(s -> s.isOnNext() && s.get()
		                                                     .equals("One"))
		            .expectNextMatches(Signal::isOnComplete)
		            .verifyComplete();
	}

	@Test
	public void materialize2() {
		StepVerifier.create(Flux.just("Three", "Two")
		                        .concatWith(Flux.error(new RuntimeException("test")))
		                        .materialize())
		            .expectNextMatches(s -> s.isOnNext() && s.get()
		                                                     .equals("Three"))
		            .expectNextMatches(s -> s.isOnNext() && s.get()
		                                                     .equals("Two"))
		            .expectNextMatches(s -> s.isOnError() && s.getThrowable()
		                                                      .getMessage()
		                                                      .equals("test"))
		            .verifyComplete();
	}

    @Test
    public void scanSubscriber() {
        Subscriber<Signal<String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxMaterialize.MaterializeSubscriber<String> test = new FluxMaterialize.MaterializeSubscriber<String>(actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        test.requested = 35;
        assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(0); // RS: TODO non-zero size

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.terminalSignal = Signal.error(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}
