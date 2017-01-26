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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxFlattenIterableTest extends AbstractFluxOperatorTest<String, String>{

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.SYNC)
		                     .prefetch(QueueSupplier.SMALL_BUFFER_SIZE)
		                     .shouldHitDropNextHookAfterTerminate(false);
	}

	@Override
	protected int fusionModeThreadBarrierSupport() {
		return Fuseable.ANY;
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorInOperatorCallback() {
		return Arrays.asList(
				scenario(f -> f.flatMapIterable(s -> null))
						.verifier(step -> step.verifyError(NullPointerException.class)),

				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s, null)))
						.verifier(step -> step.expectNext(item(0)).verifyError(NullPointerException.class)),

				scenario(f -> f.flatMapIterable(s -> null))
		             .verifier(step -> step.verifyError(NullPointerException.class))
		             .fusionMode(Fuseable.NONE)
		             .finiteFlux(Mono.fromCallable(() -> item(0)).flux()),

				scenario(f -> f.flatMapIterable(s -> null))
		             .verifier(step -> step.verifyError(NullPointerException.class))
		             .fusionMode(Fuseable.NONE)
		             .finiteFlux(Mono.fromCallable(() -> (String)null).flux()),

				scenario(f -> f.flatMapIterable(s -> () -> null))
						.verifier(step -> step.verifyError(NullPointerException.class)),

				scenario(f -> f.flatMapIterable(s -> () -> null))
						.fusionMode(Fuseable.NONE)
						.finiteFlux(Mono.fromCallable(() -> item(0)).flux())
						.verifier(step -> step.verifyError(NullPointerException.class)),

				scenario(f -> f.flatMapIterable(s -> () -> null))
						.fusionMode(Fuseable.NONE)
						.finiteFlux(Mono.fromCallable(() -> (String)null).flux())
						.verifier(step -> step.verifyError(NullPointerException.class)),

				scenario(f -> f.flatMapIterable(s -> {
					throw exception();
				})),

				scenario(f -> f.flatMapIterable(s -> {
					throw exception();
				}))
						.fusionMode(Fuseable.NONE)
						.finiteFlux(Mono.fromCallable(() -> item(0)).flux()),

				scenario(f -> f.flatMapIterable(s -> {
					throw exception();
				}))
						.fusionMode(Fuseable.NONE)
						.finiteFlux(Mono.fromCallable(() -> (String)null).flux())
						.verifier(step -> step.verifyError(NullPointerException.class)),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					@Override
					public boolean hasNext() {
						throw exception();
					}

					@Override
					public String next() {
						return null;
					}
				})),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					@Override
					public boolean hasNext() {
						throw exception();
					}

					@Override
					public String next() {
						return null;
					}
				})).fusionMode(Fuseable.NONE)
				   .finiteFlux(Mono.fromCallable(() -> item(0)).flux()),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					@Override
					public boolean hasNext() {
						throw exception();
					}

					@Override
					public String next() {
						return null;
					}
				})).fusionMode(Fuseable.NONE)
				   .finiteFlux(Mono.fromCallable(() -> (String)null).flux())
				   .verifier(step -> step.verifyError(NullPointerException.class)),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					@Override
					public boolean hasNext() {
						return true;
					}

					@Override
					public String next() {
						throw exception();
					}
				})),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					boolean invoked;
					@Override
					public boolean hasNext() {
						if(!invoked){
							return true;
						}
						throw exception();
					}

					@Override
					public String next() {
						invoked = true;
						return item(0);
					}
				}))
						.fusionMode(Fuseable.NONE)
						.verifier(step -> step.expectNext(item(0)).verifyErrorMessage("test")),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					@Override
					public boolean hasNext() {
						return true;
					}

					@Override
					public String next() {
						throw exception();
					}
				})).fusionMode(Fuseable.NONE)
				   .finiteFlux(Mono.fromCallable(() -> item(0)).flux()),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					@Override
					public boolean hasNext() {
						return true;
					}

					@Override
					public String next() {
						throw exception();
					}
				})).fusionMode(Fuseable.NONE)
				   .finiteFlux(Mono.fromCallable(() -> (String)null).flux())
				   .verifier(step -> step.verifyError(NullPointerException.class))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_threeNextAndComplete() {
		return Arrays.asList(
				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s))),

				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s), 1))
					.prefetch(1),

				scenario(f -> f.flatMapIterable(s -> new ArrayList<>()))
					.verifier(step -> step.verifyComplete()),

				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s, s + s)))
						.verifier(step -> step.expectNext(item(0), item(0)+item(0))
						                      .thenRequest(3)
						                      .expectNext(item(1), item(1)+item(1))
						                      .expectNext(item(2), item(2)+item(2))
						                      .verifyComplete())
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s, s + s)))
		);
	}

	@Test(expected=IllegalArgumentException.class)
	public void failPrefetch(){
		Flux.never()
	        .flatMapIterable(t -> null, -1);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertNoEvents();

		ts.request(1);

		ts.assertIncomplete(1);

		ts.request(2);

		ts.assertIncomplete(1, 2, 2);

		ts.request(7);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalNoFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressuredNoFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertNoEvents();

		ts.request(1);

		ts.assertIncomplete(1);

		ts.request(2);

		ts.assertIncomplete(1, 2, 2);

		ts.request(7);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void longRunning() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void longRunningNoFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void fullFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .concatMap(Flux::just)
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void just() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().concatMapIterable(v -> Arrays.asList(v, v + 1))
		                     .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

}
