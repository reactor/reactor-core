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

package reactor.test.publisher;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;
import reactor.core.publisher.ParallelFlux;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;
import reactor.util.concurrent.QueueSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.*;

/**
 * @author Stephane Maldini
 */
public abstract class BaseOperatorTest<I, PI extends Publisher<? extends I>, O, PO extends Publisher<? extends O>> {

	OperatorScenario<I, PI, O, PO> defaultScenario;

	boolean defaultEmpty = false;

	@After
	public void afterScenariosRun(){
		resetHooks();
		defaultEmpty = false;
	}

	@Before
	public final void initDefaultScenario() {
		defaultScenario = defaultScenarioOptions(new OperatorScenario<>(null, null));
	}

	@Test
	public final void cancelOnSubscribe() {
		defaultEmpty = true;
		forEachScenario(scenarios_operatorSuccess(), s -> {

			OperatorScenario<I, PI, O, PO> scenario = s.duplicate()
			                                            .receiverEmpty()
			                                            .receiverDemand(0);

			this.inputHiddenOutputBackpressured(scenario)
			    .consumeSubscriptionWith(Subscription::cancel)
			    .thenCancel()
			    .verify();

			this.inputHidden(scenario)
			    .consumeSubscriptionWith(Subscription::cancel)
			    .thenCancel()
			    .verify();

			this.inputHiddenOutputBackpressured(scenario)
			    .consumeSubscriptionWith(Subscription::cancel)
			    .thenCancel()
			    .verify();

			this.inputFusedConditionalOutputConditional(scenario)
			    .consumeSubscriptionWith(Subscription::cancel)
			    .thenCancel()
			    .verify();

			this.inputHiddenOutputConditionalCancel(scenario);

			this.inputFusedAsyncOutputFusedAsyncCancel(scenario);

			this.inputFusedAsyncOutputFusedAsyncConditionalCancel(scenario);

			this.inputFusedSyncOutputFusedSyncCancel(scenario);

			this.inputFusedSyncOutputFusedSyncConditionalCancel(scenario);

		});
	}

	@Test
	@SuppressWarnings("unchecked")
	public final void assertPrePostState() {
		forEachScenario(scenarios_touchAndAssertState(), scenario -> {
			this.inputHiddenOutputState(scenario);

			this.inputHiddenOutputConditionalState(scenario);

			this.inputFusedOutputState(scenario);

			this.inputFusedOutputConditionalState(scenario);
		});
	}

	@Test
	public final void sequenceOfNextAndComplete() {
		forEachScenario(scenarios_operatorSuccess(), scenario -> {
			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				verifier = step -> scenario.applySteps(step)
				                           .verifyComplete();
			}

			int fusion = scenario.fusionMode();

			this.inputHiddenOutputBackpressured(scenario)
			    .consumeSubscriptionWith(s -> s.request(0))
			    .verifyComplete();

			verifier.accept(this.inputHidden(scenario));
			verifier.accept(this.inputHiddenOutputConditionalTryNext(scenario));

			verifier.accept(this.inputFused(scenario));
			verifier.accept(this.inputFusedConditionalTryNext(scenario));

			if ((fusion & Fuseable.SYNC) != 0) {
				verifier.accept(this.inputFusedSyncOutputFusedSync(scenario));
				verifier.accept(this.inputFusedSyncOutputFusedSyncConditional(scenario));
			}

			if ((fusion & Fuseable.ASYNC) != 0) {
				verifier.accept(this.inputFusedAsyncOutputFusedAsync(scenario));
				verifier.accept(this.inputFusedAsyncOutputFusedAsyncConditional(scenario));
			}

			verifier.accept(this.inputConditionalTryNext(scenario));
			verifier.accept(this.inputConditionalOutputConditional(scenario));
			verifier.accept(this.inputFusedConditionalOutputConditional(scenario));
			verifier.accept(this.inputFusedConditionalOutputConditionalTryNext(scenario));

		});
	}

	@Test
	public final void sequenceOfNextWithCallbackError() {
		defaultEmpty = true;
		defaultScenario.producerError(new RuntimeException("test"));
		forEachScenario(scenarios_operatorError(), scenario -> {
			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			String m = scenario.producerError.getMessage();
			Consumer<StepVerifier.Step<O>> errorVerifier = step -> {
				try {
					step.consumeErrorWith(e -> {
						if (e instanceof NullPointerException || e instanceof IllegalStateException || e.getMessage()
						                                                                                .equals(m)) {
							return;
						}
						throw Exceptions.propagate(e);
					}).verify();
//						step.expectErrorMessage(m)
//						.verifyThenAssertThat()
//						.hasOperatorErrorWithMessage(m);
				}
				catch (Throwable e) {
					if (e instanceof AssertionError) {
						throw (AssertionError) e;
					}
					e = Exceptions.unwrap(e);
					if (e instanceof NullPointerException || e instanceof IllegalStateException || e.getMessage()
					                                                                                .equals(m)) {
						return;
					}
					throw Exceptions.propagate(e);
				}
			};

			if (verifier == null) {
				verifier = step -> errorVerifier.accept(scenario.applySteps(step));
				errorVerifier.accept(this.inputHiddenOutputBackpressured(scenario));
			}
			else {
				verifier.accept(this.inputHiddenOutputBackpressured(scenario));
			}

			int fusion = scenario.fusionMode();

			verifier.accept(this.inputHidden(scenario));
			verifier.accept(this.inputHiddenOutputConditionalTryNext(scenario));
			verifier.accept(this.inputFused(scenario));

			if (scenario.producerCount() > 0 && (fusion & Fuseable.SYNC) != 0) {
				verifier.accept(this.inputFusedSyncOutputFusedSync(scenario));
				verifier.accept(this.inputFusedSyncOutputFusedSyncConditional(scenario));
			}

			if (scenario.producerCount() > 0 && (fusion & Fuseable.ASYNC) != 0) {
				verifier.accept(this.inputFusedAsyncOutputFusedAsync(scenario));
				verifier.accept(this.inputFusedAsyncOutputFusedAsyncConditional(scenario));
				this.inputFusedAsyncOutputFusedAsyncCancel(scenario);
				this.inputFusedAsyncOutputFusedAsyncConditionalCancel(scenario);

			}

			verifier.accept(this.inputConditionalTryNext(scenario));
			verifier.accept(this.inputConditionalOutputConditional(scenario));
			verifier.accept(this.inputFusedConditionalTryNext(scenario));
			verifier.accept(this.inputFusedConditionalOutputConditional(scenario));
			verifier.accept(this.inputFusedConditionalOutputConditionalTryNext(scenario));
		});
	}

	@Test
	public final void errorOnSubscribe() {
		defaultEmpty = true;
		defaultScenario.producerError(new RuntimeException("test"));
		forEachScenario(scenarios_errorFromUpstreamFailure(), s -> {
			OperatorScenario<I, PI, O, PO> scenario = s.duplicate();

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				String m = exception().getMessage();
				verifier = step -> {
					try {
						if (scenario.shouldHitDropErrorHookAfterTerminate() || scenario.shouldHitDropNextHookAfterTerminate()) {
							StepVerifier.Assertions assertions =
									scenario.applySteps(step)
									        .expectErrorMessage(m)
									        .verifyThenAssertThat();
							if(scenario.shouldHitDropErrorHookAfterTerminate()){
								assertions.hasDroppedErrorsSatisfying(c -> {
									assertThat(c.stream().findFirst().get()).hasMessage(scenario.droppedError.getMessage());
								});
							}
							if(scenario.shouldHitDropNextHookAfterTerminate()){
								assertions.hasDropped(scenario.droppedItem);
							}
						}
						else {
							scenario.applySteps(step)
							        .verifyErrorMessage(m);
						}
					}
					catch (Exception e) {
						assertThat(Exceptions.unwrap(e)).hasMessage(m);
					}
				};
			}

			int fusion = scenario.fusionMode();

			verifier.accept(this.inputHiddenError(scenario));
			verifier.accept(this.inputHiddenErrorOutputConditional(scenario));
			verifier.accept(this.inputConditionalError(scenario));
			verifier.accept(this.inputConditionalErrorOutputConditional(scenario));
			verifier.accept(this.inputFusedError(scenario));
			verifier.accept(this.inputFusedErrorOutputFusedConditional(scenario));

			scenario.shouldHitDropErrorHookAfterTerminate(false)
			        .shouldHitDropNextHookAfterTerminate(false);

			if (scenario.prefetch() != -1 || (fusion & Fuseable.SYNC) != 0) {
				verifier.accept(this.inputFusedSyncErrorOutputFusedSync(scenario));
			}
			if (scenario.prefetch() != -1 || (fusion & Fuseable.ASYNC) != 0) {
				verifier.accept(this.inputFusedAsyncErrorOutputFusedAsync(scenario));
			}

		});
	}

	@Test
	public final void sequenceOfNextAndCancel() {
		forEachScenario(scenarios_operatorSuccess(), scenario -> {

		});
	}

	@Test
	public final void sequenceOfNextAndError() {
		forEachScenario(scenarios_operatorSuccess(), scenario -> {
		});
	}

	//common n unused item or dropped
	protected final I item(int i) {
		if (defaultScenario.producingMapper == null) {
			throw Exceptions.bubble(new Exception("No producer set in " + "defaultScenario"));
		}
		return defaultScenario.producingMapper
		                      .apply(i);
	}

	//unprocessable exception (dropped)
	protected final RuntimeException droppedException() {
		if (defaultScenario.droppedError == null) {
			throw Exceptions.bubble(new Exception("No dropped exception set in " + "defaultScenario"));
		}
		return defaultScenario.droppedError;
	}

	protected final RuntimeException exception() {
		if (defaultScenario.producerError == null) {
			throw Exceptions.bubble(new Exception("No exception set in " + "defaultScenario"));
		}
		return defaultScenario.producerError;
	}

	final int defaultLimit(OperatorScenario<I, PI, O, PO> scenario) {
		if (scenario.prefetch() == -1) {
			return QueueSupplier.SMALL_BUFFER_SIZE - (QueueSupplier.SMALL_BUFFER_SIZE >> 2);
		}
		if (scenario.prefetch() == Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		return scenario.prefetch() - (scenario.prefetch() >> 2);
	}

	protected OperatorScenario<I, PI, O, PO> defaultScenarioOptions(OperatorScenario<I, PI, O, PO> defaultOptions) {
		return defaultOptions;
	}

	protected List<? extends OperatorScenario<I, PI, O, PO>> scenarios_operatorError() {
		return Collections.emptyList();
	}

	protected List<? extends OperatorScenario<I, PI, O, PO>> scenarios_operatorSuccess() {
		return Collections.emptyList();
	}

	protected List<? extends OperatorScenario<I, PI, O, PO>> scenarios_errorFromUpstreamFailure() {
		return scenarios_operatorSuccess();
	}

	protected List<? extends OperatorScenario<I, PI, O, PO>> scenarios_touchAndAssertState() {
		return scenarios_operatorSuccess();
	}

	abstract PO conditional(PO output);

	abstract PO doOnSubscribe(PO output, Consumer<? super Subscription> doOnSubscribe);

	final PI anySource(OperatorScenario<I, PI, O, PO> scenario) {
		if((scenario.fusionMode() & Fuseable.SYNC) != 0 && scenario.producerCount() != -1){
			return withFluxSource(fluxFuseableSync(scenario));
		}
		return withFluxSource(fluxFuseableAsync(scenario));
	}

	final PI anySourceHidden(OperatorScenario<I, PI, O, PO> scenario) {
		return hide(withFluxSource(fluxFuseableAsync(scenario)));
	}

	final Flux<I> fluxFuseableAsync(OperatorScenario<I, PI, O, PO> scenario) {
		int p = scenario.producerCount();
		ReplayProcessor<I> rp = ReplayProcessor.create();

		switch (p) {
			case -1:
				break;
			case 0:
				rp.onComplete();
				break;
			case 1:
				rp.onNext(scenario.producingMapper
				                  .apply(0));
				rp.onComplete();
				break;
			default:
				if (p > 10_000) {
					throw new IllegalArgumentException("Should not preload async source" + " " + "more than 10000," + " was " + p);
				}
				for (int i = 0; i < scenario.producerCount(); i++) {
					rp.onNext(scenario.producingMapper
					                  .apply(i));
				}
				rp.onComplete();
		}
		return rp;
	}

	final Flux<I> fluxFuseableSync(OperatorScenario<I, PI, O, PO> scenario) {
		int p = scenario.producerCount();
		switch (p) {
			case -1:
				throw new IllegalArgumentException("cannot fuse sync never emitting " + "producer");
			case 0:
				return new FluxEmptySyncFuseable<>();
			default:
				return Flux.fromIterable(() -> new Iterator<I>() {
					int i = 0;

					@Override
					public boolean hasNext() {
						return i < p;
					}

					@Override
					public I next() {
						return scenario.producingMapper
						               .apply(i++);
					}
				});
		}
	}

	final <S extends OperatorScenario<I, PI, O, PO>> void forEachScenario(List<S> scenarios,
			Consumer<S> test) {
		for (S scenario : scenarios) {
			if (scenario == null) {
				continue;
			}

			try {
				test.accept(scenario);
			}
			catch (Error | RuntimeException e) {
				if (scenario.description != null) {
					e.addSuppressed(new Exception(scenario.description, scenario.stack));
				}
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw e;
			}
			catch (Throwable e) {
				if (scenario.description != null) {
					e.addSuppressed(new Exception(scenario.description, scenario.stack));
				}
				if (scenario.stack != null) {
					e.addSuppressed(scenario.stack);
				}
				throw Exceptions.bubble(e);
			}
			finally {
				resetHooks();
			}
		}
	}

	abstract PI hide(PI input);

	final StepVerifier.Step<O> inputHidden(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body().apply(anySourceHidden(scenario)));
	}

	final StepVerifier.Step<O> inputHiddenOutputBackpressured(OperatorScenario<I, PI, O, PO> scenario) {
		int expected = scenario.receiverCount();
		int missing = expected - (expected / 2);

		long toRequest =
				expected == Integer.MAX_VALUE ? Long.MAX_VALUE : (expected - missing);

		StepVerifier.Step<O> step = StepVerifier.create(scenario.body()
		                                                        .apply(anySourceHidden(scenario)), toRequest);

		if (toRequest == Long.MAX_VALUE) {
			return scenario.applySteps(step);
		}
		return scenario.applySteps(expected - missing, step);
	}



	final StepVerifier.Step<O> inputHiddenOutputConditionalTryNext(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(anySourceHidden(scenario)),
				Math.max(scenario.producerCount(), scenario.receiverCount()))
		                   .consumeSubscriptionWith(s -> s.request(0));
	}

	final void inputHiddenOutputConditionalCancel(OperatorScenario<I, PI, O, PO> scenario) {
		StepVerifier.create(scenario.body()
		                            .andThen(this::conditional)
		                            .apply(anySourceHidden(scenario)))
		            .thenCancel() //hit double cancel
		            .verify();
	}

	@SuppressWarnings("unchecked")
	final void inputHiddenOutputState(OperatorScenario<I, PI, O, PO> scenario) {
		this.fluxState(scenario, false)
		    .subscribe(Operators.drainSubscriber());
	}

	final void inputHiddenOutputConditionalState(OperatorScenario<I, PI, O, PO> scenario) {
		this.fluxState(scenario, true)
		    .subscribe(Operators.drainSubscriber());
	}

	final StepVerifier.Step<O> inputConditionalTryNext(OperatorScenario<I, PI, O, PO> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(scenario.body().apply(withFluxSource(ts.flux())), Math.max(scenario.producerCount(), scenario.receiverCount()))
		                   .then(() -> testPublisherSource(scenario, ts));
	}

	final StepVerifier.Step<O> inputConditionalOutputConditional(OperatorScenario<I, PI, O, PO> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(withFluxSource(ts.flux())))
		                   .then(() -> testPublisherSource(scenario, ts));

	}

	final StepVerifier.Step<O> inputFused(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body().apply(anySource(scenario)));
	}

	final void inputFusedOutputState(OperatorScenario<I, PI, O, PO> scenario) {
		this.fluxFuseableAsyncState(scenario, false)
		    .subscribe(Operators.drainSubscriber());
	}

	final void inputFusedOutputConditionalState(OperatorScenario<I, PI, O, PO> scenario) {
		this.fluxFuseableAsyncState(scenario, true)
		    .subscribe(Operators.drainSubscriber());
	}

	final StepVerifier.Step<O> inputFusedConditionalTryNext(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body().apply(anySource(scenario)),
				Math.max(scenario.producerCount(), scenario.receiverCount()))
		                   .consumeSubscriptionWith(s -> s.request(0));
	}

	final StepVerifier.Step<O> inputFusedConditionalOutputConditional(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(anySource(scenario)));
	}

	final StepVerifier.Step<O> inputFusedConditionalOutputConditionalTryNext(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(anySource(scenario)),
				Math.max(scenario.producerCount(), scenario.receiverCount()))
		                   .consumeSubscriptionWith(s -> s.request(0));
	}

	final StepVerifier.Step<O> inputFusedAsyncOutputFusedAsync(OperatorScenario<I, PI, O, PO> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(scenario.body()
		                                   .apply(withFluxSource(up)))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> testUnicastSource(scenario, up));
	}

	final StepVerifier.Step<O> inputFusedAsyncOutputFusedAsyncConditional(OperatorScenario<I, PI, O, PO> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(withFluxSource(up)))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> testUnicastSource(scenario, up));
	}

	@SuppressWarnings("unchecked")
	final void inputFusedAsyncOutputFusedAsyncCancel(OperatorScenario<I, PI, O, PO> scenario) {
		if ((scenario.fusionMode() & Fuseable.ASYNC) != 0) {
			UnicastProcessor<I> up = UnicastProcessor.create();
			testUnicastSource(scenario, up);
			StepVerifier.create(scenario.body()
			                            .apply(withFluxSource(up)), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs = ((Fuseable.QueueSubscription<O>) s);
					            qs.requestFusion(ASYNC);
					            if (up.actual() != qs || scenario.prefetch() == -1) {
						            qs.size(); //touch undeterministic
					            }
					            else {
						            assertThat(qs.size()).isEqualTo(up.size());
					            }
					            try {
						            qs.poll();
						            qs.poll();
						            qs.poll();
					            }
					            catch (Exception e) {
					            }
					            if (Scannable.from(qs)
					                         .scan(Scannable.ThrowableAttr.ERROR) != null) {
						            if (scenario.producerError != null) {
							            assertThat(Scannable.from(qs).scan(Scannable.ThrowableAttr.ERROR))
									            .hasMessage(scenario.producerError.getMessage());
						            }
						            if (up.actual() != qs || scenario.prefetch() == -1) {
							            qs.size(); //touch undeterministic
						            }
						            else {
							            assertThat(qs.size()).isEqualTo(up.size());
						            }
					            }
					            qs.clear();
					            assertThat(qs.size()).isEqualTo(0);
				            }
			            })
			            .thenCancel()
			            .verify();

			UnicastProcessor<I> up2 = UnicastProcessor.create();
			StepVerifier.create(scenario.body()
			                            .apply(withFluxSource(up2)), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs = ((Fuseable.QueueSubscription<O>) s);
					            assertThat(qs.requestFusion(ASYNC | THREAD_BARRIER)).isEqualTo(
							            scenario.fusionModeThreadBarrier & ASYNC);
				            }
			            })
			            .thenCancel()
			            .verify();
		}
	}

	@SuppressWarnings("unchecked")
	final void inputFusedAsyncOutputFusedAsyncConditionalCancel(OperatorScenario<I, PI, O, PO> scenario) {
		if ((scenario.fusionMode() & Fuseable.ASYNC) != 0) {
			UnicastProcessor<I> up = UnicastProcessor.create();
			testUnicastSource(scenario, up);
			StepVerifier.create(scenario.body()
			                            .andThen(f -> doOnSubscribe(f, s -> {
				                            if (s instanceof Fuseable.QueueSubscription) {
					                            Fuseable.QueueSubscription<O> qs =
							                            (Fuseable.QueueSubscription<O>) s;
					                            qs.requestFusion(ASYNC);
					                            if (up.actual() != qs || scenario.prefetch() == -1) {
						                            qs.size(); //touch undeterministic
					                            }
					                            else {
						                            assertThat(qs.size()).isEqualTo(up.size());
					                            }
					                            if (Scannable.from(qs)
					                                         .scan(Scannable.ThrowableAttr.ERROR) != null) {
						                            if (scenario.producerError != null) {
							                            assertThat(Scannable.from(qs).scan(Scannable.ThrowableAttr.ERROR))
									                            .hasMessage(scenario.producerError.getMessage());
						                            }
						                            if (up.actual() != qs || scenario.prefetch() == -1) {
							                            qs.size(); //touch undeterministic
						                            }
						                            else {
							                            assertThat(qs.size()).isEqualTo(up.size());
						                            }
					                            }
					                            qs.clear();
					                            assertThat(qs.size()).isEqualTo(0);
				                            }
			                            }))
			                            .andThen(this::conditional)
			                            .apply(withFluxSource(up)), 0)
			            .thenCancel()
			            .verify();

			UnicastProcessor<I> up2 = UnicastProcessor.create();
			StepVerifier.create(scenario.body()
			                            .andThen(f -> doOnSubscribe(f, s -> {
				                            if (s instanceof Fuseable.QueueSubscription) {
					                            Fuseable.QueueSubscription<O> qs =
							                            (Fuseable.QueueSubscription<O>) s;
					                            assertThat(qs.requestFusion(ASYNC | THREAD_BARRIER)).isEqualTo(
							                            scenario.fusionModeThreadBarrier & ASYNC);
				                            }
			                            }))
			                            .andThen(this::conditional)
			                            .apply(withFluxSource(up2)), 0)
			            .thenCancel()
			            .verify();
		}
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> inputFusedSyncOutputFusedSync(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body()
		                                   .apply(withFluxSource(fluxFuseableSync(scenario))))
		                   .expectFusion(Fuseable.SYNC);
	}

	@SuppressWarnings("unchecked")
	final void inputFusedSyncOutputFusedSyncCancel(OperatorScenario<I, PI, O, PO> scenario) {
		if (scenario.producerCount() != -1 && (scenario.fusionMode() & Fuseable.SYNC) != 0) {
			StepVerifier.create(scenario.body()
			                            .apply(withFluxSource(fluxFuseableSync(scenario))), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs =
							            ((Fuseable.QueueSubscription<O>) s);

					            assertThat(qs.requestFusion(Fuseable.SYNC | THREAD_BARRIER)).isEqualTo(
							            scenario.fusionModeThreadBarrier & Fuseable.SYNC);

					            qs.size();
					            qs.isEmpty();
					            qs.clear();
					            assertThat(qs.isEmpty()).isTrue();
				            }
			            })
			            .thenCancel()
			            .verify();

			StepVerifier.create(scenario.body()
			                            .apply(withFluxSource(fluxFuseableSync(scenario))), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs =
							            ((Fuseable.QueueSubscription<O>) s);
					            assertThat(qs.requestFusion(NONE)).isEqualTo(NONE);
				            }
			            })
			            .thenCancel()
			            .verify();
		}
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> inputFusedSyncOutputFusedSyncConditional(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(withFluxSource(fluxFuseableSync(scenario))))
		                   .expectFusion(Fuseable.SYNC);
	}

	@SuppressWarnings("unchecked")
	final void inputFusedSyncOutputFusedSyncConditionalCancel(OperatorScenario<I, PI, O, PO> scenario) {
		if (scenario.producerCount() != -1 && (scenario.fusionMode() & Fuseable.SYNC) != 0) {
			StepVerifier.create(
					scenario.body()
					        .andThen(f -> doOnSubscribe(f, s -> {
						        if (s instanceof Fuseable.QueueSubscription) {
							        Fuseable.QueueSubscription<O> qs =
									        (Fuseable.QueueSubscription<O>) s;

							        assertThat(qs.requestFusion(Fuseable.SYNC | THREAD_BARRIER)).isEqualTo(
									        scenario.fusionModeThreadBarrier & Fuseable.SYNC);

							        qs.size();
							        qs.isEmpty();
							        qs.clear();
							        assertThat(qs.isEmpty()).isTrue();
						        }
					        }))
					        .andThen(this::conditional)
					        .apply(withFluxSource(fluxFuseableSync(scenario))), 0)
			            .thenAwait()
			            .thenCancel()
			            .verify();

			StepVerifier.create(scenario.body()
			                            .andThen(f -> doOnSubscribe(f, s -> {
				                            if (s instanceof Fuseable.QueueSubscription) {
					                            Fuseable.QueueSubscription<O> qs =
							                            (Fuseable.QueueSubscription<O>) s;
					                            assertThat(qs.requestFusion(NONE)).isEqualTo(
							                            NONE);
				                            }
			                            }))
			                            .andThen(this::conditional)
			                            .apply(withFluxSource(fluxFuseableSync(scenario))), 0)
			            .thenAwait()
			            .thenCancel()
			            .verify();
		}
	}

	final StepVerifier.Step<O> inputConditionalError(OperatorScenario<I, PI, O, PO> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		return StepVerifier.create(scenario.body()
		                                   .apply(withFluxSource(ts.flux())))
		                   .then(() -> {
			                   ts.error(exception());

			                   //verify drop path
			                   if (scenario.shouldHitDropErrorHookAfterTerminate()) {
				                   ts.complete();
				                   ts.error(scenario.droppedError);
			                   }
			                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
				                   ts.next(scenario.droppedItem);
			                   }
		                   });
	}

	final StepVerifier.Step<O> inputHiddenError(OperatorScenario<I, PI, O, PO> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE,
						TestPublisher.Violation.REQUEST_OVERFLOW);
		return StepVerifier.create(scenario.body()
		                                   .apply(hide(withFluxSource(ts.flux()))))
		                   .then(() -> {
			                   ts.error(exception());
			                   if (scenario.shouldHitDropErrorHookAfterTerminate()) {
				                   ts.complete();
				                   ts.error(scenario.droppedError);
			                   }

			                   //verify drop path
			                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
				                   ts.next(scenario.droppedItem);
			                   }
		                   });
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> inputFusedError(OperatorScenario<I, PI, O, PO> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();

		return StepVerifier.create(scenario.body()
		                                   .apply(up.as(f -> withFluxSource(new FluxFuseableExceptionOnPoll<>(
				                                   f,
				                                   exception())))))
		                   .then(testUnicastDropPath(scenario, up));
	}

	final StepVerifier.Step<O> inputConditionalErrorOutputConditional(OperatorScenario<I, PI, O, PO> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(withFluxSource(ts.flux())))
		                   .then(() -> {
			                   ts.error(exception());

			                   //verify drop path
			                   if (scenario.shouldHitDropErrorHookAfterTerminate()) {
				                   ts.complete();
				                   ts.error(scenario.droppedError);
			                   }
			                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
				                   ts.next(scenario.droppedItem);
			                   }
		                   });
	}

	final StepVerifier.Step<O> inputHiddenErrorOutputConditional(OperatorScenario<I, PI, O, PO> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(hide(withFluxSource(ts.flux()))))
		                   .then(() -> {
			                   ts.error(exception());

			                   //verify drop path
			                   if (scenario.shouldHitDropNextHookAfterTerminate()) {
				                   ts.next(scenario.droppedItem);
			                   }
			                   if (scenario.shouldHitDropErrorHookAfterTerminate()) {
				                   ts.complete();
				                   ts.error(scenario.droppedError);
			                   }
		                   });
	}

	final StepVerifier.Step<O> inputFusedSyncErrorOutputFusedSync(OperatorScenario<I, PI, O, PO> scenario) {
		return StepVerifier.create(scenario.body()
		                                   .apply(Flux.just(item(0), item(1))
		                                              .as(f -> withFluxSource(new FluxFuseableExceptionOnPoll<>(
				                                              f,
				                                              exception())))))
		                   .expectFusion(scenario.fusionMode() & SYNC);
	}

	final StepVerifier.Step<O> inputFusedAsyncErrorOutputFusedAsync(OperatorScenario<I, PI, O, PO> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		up.onNext(item(0));
		return StepVerifier.create(scenario.body()
		                                   .apply(up.as(f -> withFluxSource(new FluxFuseableExceptionOnPoll<>(
				                                   f,
				                                   exception())))))
		                   .expectFusion(scenario.fusionMode() & ASYNC);
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> inputFusedErrorOutputFusedConditional(OperatorScenario<I, PI, O, PO> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(scenario.body()
		                                   .andThen(this::conditional)
		                                   .apply(up.as(f -> withFluxSource(new FluxFuseableExceptionOnPoll<>(
				                                   f,
				                                   exception())))))
		                   .then(testUnicastDropPath(scenario, up));
	}

	final Runnable testUnicastDropPath(OperatorScenario<I, PI, O, PO> scenario,
			UnicastProcessor<I> up) {
		return () -> {
			if (up.actual() != null) {
				up.actual()
				  .onError(exception());

				//verify drop path
				if (scenario.shouldHitDropErrorHookAfterTerminate()) {
					up.actual()
					  .onComplete();
					up.actual()
					  .onError(scenario.droppedError);
				}
				if (scenario.shouldHitDropNextHookAfterTerminate()) {
					FluxFuseableExceptionOnPoll.next(up.actual(), scenario.droppedItem);

					if (FluxFuseableExceptionOnPoll.shouldTryNext(up.actual())) {
						FluxFuseableExceptionOnPoll.tryNext(up.actual(), scenario.droppedItem);
					}
				}

			}
		};
	}

	final void touchInner(@Nullable Object t){
		if(t == null) return;
		Scannable o = Scannable.from(t);
		o.scan(Scannable.ScannableAttr.ACTUAL);
		o.scan(Scannable.IntAttr.BUFFERED);
		o.scan(Scannable.BooleanAttr.CANCELLED);
		o.scan(Scannable.IntAttr.CAPACITY);
		o.scan(Scannable.BooleanAttr.DELAY_ERROR);
		o.scan(Scannable.ThrowableAttr.ERROR);
		o.scan(Scannable.IntAttr.PREFETCH);
		o.scan(Scannable.ScannableAttr.PARENT);
		o.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM);
		o.scan(Scannable.BooleanAttr.TERMINATED);
		o.inners();
	}

	@SuppressWarnings("unchecked")
	final void touchTreeState(@Nullable Object parent){
		if (parent == null) {
			return;
		}
		touchInner(parent);
		Scannable.from(parent)
		         .inners()
		         .forEach(this::touchInner);
	}

	final void resetHooks() {
		Hooks.resetOnErrorDropped();
		Hooks.resetOnNextDropped();
		Hooks.resetOnOperator();
		Hooks.resetOnOperatorError();
	}

	final  void testPublisherSource(OperatorScenario<I, PI, O, PO> scenario, TestPublisher<I> ts) {
		fluxFuseableAsync(scenario).subscribe(ts::next, ts::error, ts::complete);
	}

	final void testUnicastSource(OperatorScenario<I, PI, O, PO> scenario,
			UnicastProcessor<I> ts) {
		fluxFuseableAsync(scenario).subscribe(ts);
	}

	abstract PI sourceCallable(OperatorScenario<I, PI, O, PO> scenario);

	abstract PI sourceScalar(OperatorScenario<I, PI, O, PO> scenario);

	abstract PI withFluxSource(Flux<I> input);

	@SuppressWarnings("unchecked")
	final Flux<O> fluxFuseableAsyncState(OperatorScenario<I, PI, O, PO> scenario,
			boolean conditional) {
		AtomicReference<Scannable> ref = new AtomicReference<>();
		Flux<I> source = this.fluxFuseableAsync(scenario)
		                     .doOnSubscribe(s -> Scannable.from(s)
		                                                  .actuals()
		                                                  .skip(1)
		                                                  .findFirst()
		                                                  .ifPresent(t -> {
			                                                  ref.set(t);
			                                                 if (scenario.prefetch() != -1) {
				                                                  assertThat(t.scan(Scannable.IntAttr.PREFETCH))
						                                                  .isEqualTo(scenario.prefetch());
			                                                  }
		                                                  }));

		if (source.getPrefetch() != -1 && scenario.prefetch() != -1) {
			assertThat(Math.min(source.getPrefetch(), Integer.MAX_VALUE)).isEqualTo(
					scenario.prefetch());
		}

		PO f;

		f = applyStateScenario(scenario, conditional, source);

		return Flux.from(f)
		           .doOnSubscribe(parent -> {
			           Scannable t = Scannable.from(parent);
			           assertThat(t.scan(Scannable.ThrowableAttr.ERROR)).isNull();
			           assertThat(t.scanOrDefault(Scannable.BooleanAttr.TERMINATED, false)).isFalse();

			           //noop path
			           if (parent instanceof Subscriber) {
				           ((Subscriber<I>) parent).onSubscribe(Operators.emptySubscription());
				           ((Subscriber<I>) parent).onSubscribe(Operators.cancelledSubscription());
			           }

			           touchTreeState(parent);
		           })
		           .doOnComplete(() -> {
			           if (ref.get() != null) {
				           Scannable t = ref.get();
				           if (scenario.shouldAssertPostTerminateState()) {
					           assertThat(t.scanOrDefault(Scannable.BooleanAttr.TERMINATED, true)).isTrue();
				           }
				           touchTreeState(ref.get());
			           }
		           })
		           .doOnNext(d -> touchTreeState(ref.get()));
	}

	final PO fluxState(OperatorScenario<I, PI, O, PO> scenario, boolean conditional) {
		Flux<I> source = Flux.from(s -> {
			Scannable t = Scannable.from(s);
			assertThat(t.scan(Scannable.ThrowableAttr.ERROR)).isNull();
			assertThat(t.scanOrDefault(Scannable.BooleanAttr.TERMINATED, false)).isFalse();

				if (scenario.prefetch() != -1) {
					assertThat(t.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(scenario.prefetch());
				}

			touchTreeState(s);

			s.onSubscribe(Operators.emptySubscription());
			s.onSubscribe(Operators.emptySubscription()); //noop path
			s.onSubscribe(Operators.cancelledSubscription()); //noop path
			s.onComplete();
			touchTreeState(s);
			if (scenario.shouldAssertPostTerminateState()) {
				assertThat(t.scanOrDefault(Scannable.BooleanAttr.TERMINATED, true)).isTrue();
			}
		});

		return applyStateScenario(scenario, conditional, source);
	}

	PO applyStateScenario(OperatorScenario<I, PI, O, PO> scenario,
			boolean conditional,
			Flux<I> source) {
		PO f;
		if (conditional) {
			f = scenario.body()
			            .andThen(this::conditional)
			            .apply(withFluxSource(source));
		}
		else {
			f = scenario.body()
			            .apply(withFluxSource(source));
			if ((f instanceof Flux || f instanceof ParallelFlux) && scenario.prefetch() != -1) {
				if (f instanceof Flux) {
					assertThat(Math.min(((Flux) f).getPrefetch(),
							Integer.MAX_VALUE)).isEqualTo(scenario.prefetch());
				}
				else {
					assertThat(Math.min(((ParallelFlux) f).getPrefetch(),
							Integer.MAX_VALUE)).isEqualTo(scenario.prefetch());
				}
			}
		}
		return f;
	}

}
