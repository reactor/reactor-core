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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractFluxOperatorTest<I, O> {

	public interface Scenario<I, O> {

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario) {
			return from(scenario, Fuseable.NONE);
		}

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode) {
			return from(scenario, fusionMode, null);
		}

		@SuppressWarnings("unchecked")
		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Consumer<StepVerifier.Step<O>> verifier) {
			return from(scenario,
					fusionMode,
					(Flux<I>) Flux.just("test", "test2", "test3"),
					verifier);
		}

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Flux<I> finiteSource,
				Consumer<StepVerifier.Step<O>> verifier) {
			return new SimpleScenario<>(scenario, fusionMode, finiteSource, verifier);
		}

		Function<Flux<I>, Flux<O>> body();

		int fusionMode();

		Consumer<StepVerifier.Step<O>> verifier();

		Flux<I> finiteSource();
	}

	static final class SimpleScenario<I, O> implements Scenario<I, O> {

		final Function<Flux<I>, Flux<O>>     scenario;
		final int                            fusionMode;
		final Consumer<StepVerifier.Step<O>> verifier;
		final Flux<I>                        finiteSource;

		SimpleScenario(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Flux<I> finiteSource,
				Consumer<StepVerifier.Step<O>> verifier) {
			this.scenario = scenario;
			this.fusionMode = fusionMode;
			this.verifier = verifier;
			this.finiteSource = finiteSource;
		}

		@Override
		public Function<Flux<I>, Flux<O>> body() {
			return scenario;
		}

		@Override
		public int fusionMode() {
			return fusionMode;
		}

		@Override
		public Consumer<StepVerifier.Step<O>> verifier() {
			return verifier;
		}

		@Override
		public Flux<I> finiteSource() {
			return finiteSource;
		}
	}

	@Test
	public void errorWithUserProvidedCallback() {
		for (Scenario<I, O> scenario : errorInOperatorCallback()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				verifier = step -> step.verifyErrorMessage("test");
			}

			int fusion = scenario.fusionMode();

			verifier.accept(this.operatorVerifier(scenario));

			verifier.accept(this.operatorVerifierFused(scenario));

			if ((fusion & Fuseable.SYNC) != 0) {
				verifier.accept(this.operatorVerifierFusedSync(scenario));
				verifier.accept(this.operatorVerifierFusedConditionalSync(scenario));
			}

			if ((fusion & Fuseable.ASYNC) != 0) {
				verifier.accept(this.operatorVerifierFusedAsync(scenario));
				verifier.accept(this.operatorVerifierFusedConditionalAsync(scenario));
			}

			verifier.accept(this.operatorVerifierTryNext(scenario));
			verifier.accept(this.operatorVerifierBothConditional(scenario));
			verifier.accept(this.operatorVerifierConditionalTryNext(scenario));
			verifier.accept(this.operatorVerifierFusedTryNext(scenario));
			verifier.accept(this.operatorVerifierFusedBothConditional(scenario));
			verifier.accept(this.operatorVerifierFusedBothConditionalTryNext(scenario));

		}
	}

	@Test
	public void errorWithUpstreamFailure() {
		for (Scenario<I, O> scenario : errorFromUpstreamFailure()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				verifier = step -> step.verifyErrorMessage("test");
			}

			verifier.accept(this.operatorErrorSourceVerifier(scenario));
			verifier.accept(this.operatorErrorSourceVerifierTryNext(scenario));
			verifier.accept(this.operatorErrorSourceVerifierFused(scenario));
			verifier.accept(this.operatorErrorSourceVerifierConditional(scenario));
			verifier.accept(this.operatorErrorSourceVerifierConditionalTryNext(scenario));
			verifier.accept(this.operatorErrorSourceVerifierFusedBothConditional(scenario));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void assertPrePostState() {
		for (Scenario<I, O> scenario : simpleAssert()) {
			if (scenario == null) {
				continue;
			}

			Flux<O> f = Flux.<I>from(s -> {
				Trackable t = null;
				if (s instanceof Trackable) {
					t = (Trackable) s;
					assertThat(t.getError()).isNull();
					assertThat(t.isStarted()).isFalse();
					assertThat(t.isTerminated()).isFalse();
				}

				if (s instanceof Receiver) {
					assertThat(((Receiver) s).upstream()).isNull();
				}

				if (s instanceof Loopback) {
					assertThat(((Loopback) s).connectedInput()).isNotNull();
				}

				if (s instanceof Producer) {
					assertThat(((Producer) s).downstream()).isNotNull();
				}

				s.onSubscribe(Operators.emptySubscription());
				s.onSubscribe(Operators.emptySubscription()); //noop path
				if (t != null) {
					assertThat(t.isStarted()).isTrue();
				}
				s.onComplete();
				if (t != null) {
					assertThat(t.isStarted()).isFalse();
					assertThat(t.isTerminated()).isTrue();
				}
			}).as(scenario.body());

			f.subscribe();

			f.filter(d -> true)
			 .subscribe();

			AtomicReference<Trackable> ref = new AtomicReference<>();
			f = scenario.finiteSource()
			            .doOnSubscribe(s -> {
				            Object _s =
						            ((Producer) ((Producer) s).downstream()).downstream();
				            if (_s instanceof Trackable) {
					            Trackable t = (Trackable) _s;
					            ref.set(t);
					            assertThat(t.isStarted()).isFalse();
				            }
			            })
			            .as(scenario.body())
			            .doOnSubscribe(parent -> {
				            if (parent instanceof Trackable) {
					            Trackable t = (Trackable) parent;
					            assertThat(t.getError()).isNull();
					            assertThat(t.isStarted()).isTrue();
					            assertThat(t.isTerminated()).isFalse();
				            }

				            //noop path
				            if (parent instanceof Subscriber) {
					            ((Subscriber<I>) parent).onSubscribe(Operators.emptySubscription());
				            }

				            if (parent instanceof Receiver) {
					            assertThat(((Receiver) parent).upstream()).isNotNull();
				            }

				            if (parent instanceof Loopback) {
					            assertThat(((Loopback) parent).connectedInput()).isNotNull();
				            }

				            if (parent instanceof Producer) {
					            assertThat(((Producer) parent).downstream()).isNotNull();
				            }
			            })
			            .doOnComplete(() -> {
				            if (ref.get() != null) {
					            assertThat(ref.get()
					                          .isStarted()).isFalse();
					            assertThat(ref.get()
					                          .isTerminated()).isTrue();
				            }
			            });

			f.subscribe();

			f.filter(t -> true)
			 .subscribe();
		}
	}

	//errorInOperatorCallbackVerification
	protected List<Scenario<I, O>> errorInOperatorCallback() {
		return Collections.emptyList();
	}

	//assert
	protected List<Scenario<I, O>> simpleAssert() {
		return errorFromUpstreamFailure();
	}

	//errorFromUpstreamFailureVerification
	protected List<Scenario<I, O>> errorFromUpstreamFailure() {
		return Collections.emptyList();
	}

	//common source emitting once
	@SuppressWarnings("unchecked")
	protected void testPublisherSource(TestPublisher<I> ts) {
		ts.next((I) "test");
	}

	//common first unused item or dropped
	@SuppressWarnings("unchecked")
	protected I droppableItem() {
		return (I) "dropped";
	}

	final StepVerifier.Step<O> operatorVerifier(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource().hide()
		                                                 .as(scenario.body()), 2);
	}

	final StepVerifier.Step<O> operatorVerifierTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body()))
		                   .then(() -> testPublisherSource(ts));
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("dropped");
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .as(scenario.body()))
		                   .then(() -> {
			                   ts.error(new Exception("test"));

			                   //verify drop path
			                   ts.error(new Exception("dropped"));
			                   ts.next(droppableItem());
			                   ts.complete();
			                   assertThat(errorDropped.get()).isTrue();
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifier(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .hide()
		                             .as(scenario.body()))
		                   .then(() -> {
			                   ts.error(new Exception("test"));

			                   //verify drop path
			                   ts.next(droppableItem());
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorVerifierFused(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body()));
	}

	final StepVerifier.Step<O> operatorVerifierFusedSync(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body()))
		                   .expectFusion(Fuseable.SYNC);
	}

	final StepVerifier.Step<O> operatorVerifierFusedConditionalSync(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body())
		                                   .filter(d -> true))
		                   .expectFusion(Fuseable.SYNC);
	}

	final StepVerifier.Step<O> operatorVerifierFusedAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(up.as(scenario.body()))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> up.onNext(droppableItem()));
	}

	final StepVerifier.Step<O> operatorVerifierFusedConditionalAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(up.as(scenario.body())
		                             .filter(d -> true))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> up.onNext(droppableItem()));
	}

	final StepVerifier.Step<O> operatorVerifierFusedTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body()), 2);
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> operatorErrorSourceVerifierFused(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("dropped");
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(up.as(scenario.body()))
		                   .then(() -> {
			                   up.actual.onError(new Exception("test"));

			                   //verify drop path
			                   up.actual.onError(new Exception("dropped"));
			                   assertThat(errorDropped.get()).isTrue();

			                   up.actual.onNext(droppableItem());
			                   assertThat(nextDropped.get()).isTrue();
			                   if (up.actual instanceof Fuseable.ConditionalSubscriber) {
				                   nextDropped.set(false);
				                   ((Fuseable.ConditionalSubscriber<I>) up.actual).tryOnNext(
						                   droppableItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
			                   up.actual.onComplete();
		                   });
	}

	final StepVerifier.Step<O> operatorVerifierConditionalTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .hide()
		                                   .as(scenario.body())
		                                   .filter(d -> true), 2);
	}

	final StepVerifier.Step<O> operatorVerifierBothConditional(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body())
		                             .filter(d -> true))
		                   .then(() -> testPublisherSource(ts));
	}

	final StepVerifier.Step<O> operatorVerifierFusedBothConditional(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body())
		                                   .filter(d -> true));
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierConditionalTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("dropped");
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .as(scenario.body())
		                             .filter(d -> true))
		                   .then(() -> {
			                   ts.error(new Exception("test"));

			                   //verify drop path
			                   ts.error(new Exception("dropped"));
			                   ts.next(droppableItem());
			                   ts.complete();
			                   assertThat(errorDropped.get()).isTrue();
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierConditional(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .hide()
		                             .as(scenario.body())
		                             .filter(d -> true))
		                   .then(() -> {
			                   ts.error(new Exception("test"));

			                   //verify drop path
			                   ts.next(droppableItem());
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorVerifierFusedBothConditionalTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body())
		                                   .filter(d -> true), 2);
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> operatorErrorSourceVerifierFusedBothConditional(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("dropped");
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(up.as(scenario.body())
		                             .filter(d -> true))
		                   .then(() -> {
			                   up.actual.onError(new Exception("test"));

			                   //verify drop path
			                   up.actual.onError(new Exception("dropped"));
			                   assertThat(errorDropped.get()).isTrue();

			                   up.actual.onNext(droppableItem());
			                   assertThat(nextDropped.get()).isTrue();

			                   if (up.actual instanceof Fuseable.ConditionalSubscriber) {
				                   nextDropped.set(false);
				                   ((Fuseable.ConditionalSubscriber<I>) up.actual).tryOnNext(
						                   droppableItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
			                   up.actual.onComplete();
		                   });
	}

	@After
	public void resetHooks() {
		Hooks.resetOnErrorDropped();
		Hooks.resetOnNextDropped();
		Hooks.resetOnOperator();
		Hooks.resetOnOperatorError();
	}
}
