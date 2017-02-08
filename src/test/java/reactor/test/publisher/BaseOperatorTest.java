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
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.MultiProducer;
import reactor.core.MultiReceiver;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.ASYNC;
import static reactor.core.Fuseable.NONE;
import static reactor.core.Fuseable.THREAD_BARRIER;
import static reactor.core.Trackable.UNSPECIFIED;

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

			this.inputHiddenOutputConditionalCancel(scenario);

			this.inputFusedAsyncOutputFusedAsyncCancel(scenario);

			this.inputFusedAsyncOutputFusedAsyncConditionalCancel(scenario);

			this.inputFusedSyncOutputFusedSyncCancel(scenario);

			this.inputFusedSyncOutputFusedSyncConditionalCancel(scenario);

			this.inputFusedConditionalOutputConditional(scenario)
			    .consumeSubscriptionWith(Subscription::cancel)
			    .thenCancel()
			    .verify();

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

			String m = scenario.producerError().getMessage();
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
	protected I item(int i) {
		return defaultScenario.producer()
		                      .apply(i);
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

	abstract PO conditional(PO output);

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
				rp.onNext(scenario.producer()
				                  .apply(0));
				rp.onComplete();
				break;
			default:
				if (p > 10_000) {
					throw new IllegalArgumentException("Should not preload async source" + " " + "more than 10000," + " was " + p);
				}
				for (int i = 0; i < scenario.producerCount(); i++) {
					rp.onNext(scenario.producer()
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
						return scenario.producer()
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
					            if (up.downstream() != qs || scenario.prefetch() == UNSPECIFIED) {
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
					            if (qs instanceof Trackable && ((Trackable) qs).getError() != null) {
						            if (scenario.producerError() != null) {
							            assertThat(((Trackable) qs).getError()).hasMessage(
									            scenario.producerError()
									                    .getMessage());
						            }
						            if (up.downstream() != qs || scenario.prefetch() == UNSPECIFIED) {
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
							            scenario.fusionModeThreadBarrier() & ASYNC);
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
			                            .andThen(this::conditional)
			                            .apply(withFluxSource(up)), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs = ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());
					            qs.requestFusion(ASYNC);
					            if (up.downstream() != qs || scenario.prefetch() == UNSPECIFIED) {
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
					            if (qs instanceof Trackable && ((Trackable) qs).getError() != null) {
						            if (scenario.producerError() != null) {
							            assertThat(((Trackable) qs).getError()).hasMessage(
									            scenario.producerError()
									                    .getMessage());
						            }
						            if (up.downstream() != qs || scenario.prefetch() == UNSPECIFIED) {
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
			                            .andThen(this::conditional)
			                            .apply(withFluxSource(up2)), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs = ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());
					            assertThat(qs.requestFusion(ASYNC | THREAD_BARRIER)).isEqualTo(
							            scenario.fusionModeThreadBarrier() & ASYNC);
				            }
			            })
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
							            scenario.fusionModeThreadBarrier() & Fuseable.SYNC);

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
					        .andThen(this::conditional)
					        .apply(withFluxSource(fluxFuseableSync(scenario))), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs =
							            ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());

					            assertThat(qs.requestFusion(Fuseable.SYNC | THREAD_BARRIER)).isEqualTo(
							            scenario.fusionModeThreadBarrier() & Fuseable.SYNC);

					            qs.size();
					            qs.isEmpty();
					            qs.clear();
					            assertThat(qs.isEmpty()).isTrue();
				            }
			            })
			            .thenCancel()
			            .verify();

			StepVerifier.create(scenario.body()
			                            .andThen(this::conditional)
			                            .apply(withFluxSource(fluxFuseableSync(scenario))), 0)
			            .consumeSubscriptionWith(s -> {
				            if (s instanceof Fuseable.QueueSubscription) {
					            Fuseable.QueueSubscription<O> qs =
							            ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());
					            assertThat(qs.requestFusion(NONE)).isEqualTo(NONE);
				            }
			            })
			            .thenCancel()
			            .verify();
		}
	}

	final void touchInner(Object t){
		if(t instanceof Trackable){
			Trackable o = (Trackable)t;
			o.requestedFromDownstream();
			o.expectedFromUpstream();
			o.getPending();
			o.getCapacity();
			o.getError();
			o.limit();
			o.isTerminated();
			o.isStarted();
			o.isCancelled();
		}
		if(t instanceof Producer){
			((Producer)t).downstream();
		}
		if(t instanceof Loopback){
			((Loopback)t).connectedInput();
			((Loopback)t).connectedOutput();
		}
		if(t instanceof Receiver){
			((Receiver)t).upstream();
		}
	}

	@SuppressWarnings("unchecked")
	final void touchTreeState(Object parent){

		if (parent == null) {
			return;
		}
		if (parent instanceof Loopback) {
			assertThat(((Loopback) parent).connectedInput()).isNotNull();
			((Loopback) parent).connectedOutput();
		}

		if (parent instanceof Producer) {
			assertThat(((Producer) parent).downstream()).isNotNull();
		}

		if (parent instanceof MultiProducer){
			MultiProducer p = ((MultiProducer)parent);
			if(p.downstreamCount() != 0 || p.hasDownstreams()){
				Iterator<?> it = p.downstreams();
				while(it.hasNext()){
					touchInner(it.next());
				}
			}
		}

		if(parent instanceof MultiReceiver){
			MultiReceiver p = ((MultiReceiver)parent);
			if(p.upstreamCount() != 0){
				Iterator<?> it = p.upstreams();
				while(it.hasNext()){
					touchInner(it.next());
				}
			}
		}
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

}
