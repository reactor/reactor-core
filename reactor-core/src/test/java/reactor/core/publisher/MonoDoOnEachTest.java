/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MonoDoOnEachTest {

	@Test
	public void nullSource() {
		Assertions.assertThatNullPointerException()
		          .isThrownBy(() -> new MonoDoOnEach<>(null, s -> {}))
		          .withMessage(null);
	}

	@Test
	public void nullConsumer() {
		Assertions.assertThatNullPointerException()
		          .isThrownBy(() -> new MonoDoOnEach<>(Mono.just("foo"), null))
		          .withMessage("onSignal");
	}

	@Test
	public void usesFluxDoOnEachSubscriber() {
		@SuppressWarnings("unchecked")
		ArgumentCaptor<CoreSubscriber<String>> argumentCaptor =
				ArgumentCaptor.forClass(CoreSubscriber.class);
		@SuppressWarnings("unchecked")
		Mono<String> source = Mockito.mock(Mono.class);

		final MonoDoOnEach<String> test =
				new MonoDoOnEach<>(source, s -> { });

		test.subscribe();
		Mockito.verify(source).subscribe(argumentCaptor.capture());

		assertThat(argumentCaptor.getValue()).isInstanceOf(FluxDoOnEach.DoOnEachSubscriber.class);
	}

	@Test
	public void usesFluxDoOnEachConditionalSubscriber() {
		AtomicReference<Scannable> ref = new AtomicReference<>();
		Mono<String> source = Mono.just("foo")
		                          .doOnSubscribe(sub -> ref.set(Scannable.from(sub)))
		                          .hide()
		                          .filter(t -> true);

		final MonoDoOnEach<String> test =
				new MonoDoOnEach<>(source, s -> { });

		test.filter(t -> true)
		    .subscribe();

		Class<?> expected = FluxDoOnEach.DoOnEachConditionalSubscriber.class;
		Stream<Class<?>> streamOfClasses = ref.get().actuals().map(Object::getClass);

		assertThat(streamOfClasses).contains(expected);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger onNext = new AtomicInteger();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();

		Mono.just(1)
		    .hide()
		    .doOnEach(s -> {
			    if (s.isOnNext()) {
				    onNext.incrementAndGet();
			    }
			    else if (s.isOnError()) {
				    onError.set(s.getThrowable());
			    }
			    else if (s.isOnComplete()) {
				    onComplete.set(true);
			    }
		    })
		    .subscribe(ts);

		assertThat(onNext).hasValue(1);
		assertThat(onError.get()).isNull();
		assertThat(onComplete.get()).isTrue();
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger onNext = new AtomicInteger();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();

		Mono.<Integer>error(new RuntimeException("forced failure"))
				.doOnEach(s -> {
					if (s.isOnNext()) {
						onNext.incrementAndGet();
					}
					else if (s.isOnError()) {
						onError.set(s.getThrowable());
					}
					else if (s.isOnComplete()) {
						onComplete.set(true);
					}
				})
				.subscribe(ts);

		assertThat(onNext.get()).isZero();
		assertThat(onError.get()).isInstanceOf(RuntimeException.class)
		                         .hasMessage("forced failure");
		assertThat(onComplete.get()).isFalse();
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger onNext = new AtomicInteger();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();

		Mono.<Integer>empty()
				.doOnEach(s -> {
					if (s.isOnNext()) {
						onNext.incrementAndGet();
					}
					else if (s.isOnError()) {
						onError.set(s.getThrowable());
					}
					else if (s.isOnComplete()) {
						onComplete.set(true);
					}
				})
				.subscribe(ts);

		assertThat(onNext.get()).isZero();
		assertThat(onError.get()).isNull();
		assertThat(onComplete.get()).isTrue();
	}

	@Test
	public void never() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger onNext = new AtomicInteger();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();

		Mono.<Integer>never()
				.doOnEach(s -> {
					if (s.isOnNext()) {
						onNext.incrementAndGet();
					}
					else if (s.isOnError()) {
						onError.set(s.getThrowable());
					}
					else if (s.isOnComplete()) {
						onComplete.set(true);
					}
				})
				.subscribe(ts);

		assertThat(onNext.get()).isZero();
		assertThat(onError.get()).isNull();
		assertThat(onComplete.get()).isFalse();
	}

	@Test
	public void consumerError() {
		LongAdder state = new LongAdder();
		Throwable err = new Exception("test");

		StepVerifier.create(
				Mono.just(1)
				    .doOnEach(s -> {
					    if (s.isOnNext()) {
						    state.increment();
						    throw Exceptions.propagate(err);
					    }
				    }))
		            .expectErrorMessage("test")
		            .verify();

		assertThat(state.intValue()).isEqualTo(1);
	}

	@Test
	public void consumerBubbleError() {
		LongAdder state = new LongAdder();
		Throwable err = new Exception("test");

		assertThatThrownBy(() ->
				StepVerifier.create(
						Mono.just(1)
						    .doOnEach(s -> {
							    if (s.isOnNext()) {
								    state.increment();
								    throw Exceptions.bubble(err);
							    }
						    }))
				            .expectErrorMessage("test")
				            .verify())
				.isInstanceOf(RuntimeException.class)
				.matches(Exceptions::isBubbling, "bubbling")
				.hasCause(err); //equivalent to unwrap for this case
		assertThat(state.intValue()).isEqualTo(1);
	}

	@Test
	public void nextComplete() {
		List<Tuple2<Signal, ContextView>> signalsAndContext = new ArrayList<>();
		Mono.just(1)
		    .hide()
		    .doOnEach(s -> signalsAndContext.add(Tuples.of(s, s.getContextView())))
		    .contextWrite(Context.of("foo", "bar"))
		    .subscribe();

		assertThat(signalsAndContext)
				.hasSize(2)
				.allSatisfy(t2 -> {
					assertThat(t2.getT1())
							.isNotNull();
					assertThat(t2.getT2().getOrDefault("foo", "baz"))
							.isEqualTo("bar");
				});

		assertThat(signalsAndContext.stream().map(t2 -> t2.getT1().getType()))
				.containsExactly(SignalType.ON_NEXT, SignalType.ON_COMPLETE);
	}

	@Test
	public void nextError() {
		List<Tuple2<Signal, ContextView>> signalsAndContext = new ArrayList<>();
		Mono.just(0)
		    .map(i -> 10 / i)
		    .doOnEach(s -> signalsAndContext.add(Tuples.of(s,s.getContextView())))
		    .contextWrite(Context.of("foo", "bar"))
		    .subscribe();

		assertThat(signalsAndContext)
				.hasSize(1)
				.allSatisfy(t2 -> {
					assertThat(t2.getT1())
							.isNotNull();
					assertThat(t2.getT2().getOrDefault("foo", "baz"))
							.isEqualTo("bar");
				});

		assertThat(signalsAndContext.stream().map(t2 -> t2.getT1().getType()))
				.containsExactly(SignalType.ON_ERROR);
	}

	//see https://github.com/reactor/reactor-core/issues/1547
	@Test
	public void triggersCompleteSignalInMonoOnNext() {
		CopyOnWriteArrayList<String> eventOrder = new CopyOnWriteArrayList<>();

		Mono.just("parent").hide()
		    .flatMap(v -> Mono.just("child").hide()
				    .doOnEach(sig -> eventOrder.add("childEach" + sig))
				    .doOnSuccess(it -> eventOrder.add("childSuccess"))
		    )
		    .doOnEach(sig -> eventOrder.add("parentEach" + sig))
		    .doOnSuccess(it -> eventOrder.add("parentSuccess"))
		    .block();

		assertThat(eventOrder).containsExactly(
				"childEachdoOnEach_onNext(child)",
				"childEachonComplete()",
				"childSuccess",
				"parentEachdoOnEach_onNext(child)",
				"parentEachonComplete()",
				"parentSuccess");
	}

	//see https://github.com/reactor/reactor-core/issues/1547
	@Test
	public void triggersCompleteSignalInMonoOnNextFused() {
		CopyOnWriteArrayList<String> eventOrder = new CopyOnWriteArrayList<>();

		Mono.just("parent")
		    .flatMap(v -> Mono.just("child")
				    .doOnEach(sig -> eventOrder.add("childEach" + sig))
				    .doOnSuccess(it -> eventOrder.add("childSuccess"))
		    )
		    .doOnEach(sig -> eventOrder.add("parentEach" + sig))
		    .doOnSuccess(it -> eventOrder.add("parentSuccess"))
		    .block();

		assertThat(eventOrder).containsExactly(
				"childEachdoOnEach_onNext(child)",
				"childEachonComplete()",
				"childSuccess",
				"parentEachdoOnEach_onNext(child)",
				"parentEachonComplete()",
				"parentSuccess");
	}

	//see https://github.com/reactor/reactor-core/issues/1547
	@Test
	@Disabled("Mono doesn't trigger tryOnNext")
	public void triggersCompleteSignalInMonoOnNextConditional() {
		CopyOnWriteArrayList<String> eventOrder = new CopyOnWriteArrayList<>();

		Mono.just("parent").hide()
		    .filter(it -> true)
		    .flatMap(v -> Mono.just("child").hide()
		                      .doOnEach(sig -> eventOrder.add("childEach" + sig))
		                      .doOnSuccess(it -> eventOrder.add("childSuccess"))
		                      .filter(it -> true)
		    )
		    .doOnEach(sig -> eventOrder.add("parentEach" + sig))
		    .doOnSuccess(it -> eventOrder.add("parentSuccess"))
		    .filter(it -> true)
		    .block();

		assertThat(eventOrder).containsExactly(
				"childEachdoOnEach_onNext(child)",
				"childEachonComplete()",
				"childSuccess",
				"parentEachdoOnEach_onNext(child)",
				"parentEachonComplete()",
				"parentSuccess");
	}

	//see https://github.com/reactor/reactor-core/issues/1547
	@Test
	@Disabled("Mono doesn't trigger tryOnNext")
	public void triggersCompleteSignalInMonoOnNextConditionalFused() {
		CopyOnWriteArrayList<String> eventOrder = new CopyOnWriteArrayList<>();

		Mono.just("parent")
		    .flatMap(v -> Mono.just("child")
		                      .doOnEach(sig -> eventOrder.add("childEach" + sig))
		                      .doOnSuccess(it -> eventOrder.add("childSuccess"))
		                      .filter(it -> true)
		    )
		    .doOnEach(sig -> eventOrder.add("parentEach" + sig))
		    .doOnSuccess(it -> eventOrder.add("parentSuccess"))
		    .filter(it -> true)
		    .block();

		assertThat(eventOrder).containsExactly(
				"childEachdoOnEach_onNext(child)",
				"childEachonComplete()",
				"childSuccess",
				"parentEachdoOnEach_onNext(child)",
				"parentEachonComplete()",
				"parentSuccess");
	}

	@Test
	public void errorDuringCompleteSignalInMonoOnNext() {
		CopyOnWriteArrayList<String> eventOrder = new CopyOnWriteArrayList<>();

		Mono.fromSupplier(() -> "parent").hide()
		    .flatMap(ignored -> Mono.fromSupplier(() -> "child").hide()
				    .doOnEach(sig -> {
				    	eventOrder.add("childEach" + sig);
				    	if (sig.isOnNext()) {
				    		throw new IllegalStateException("boomChild");
					    }
				    })
				    .doOnSuccess(v -> eventOrder.add("childSuccess"))
				    .doOnError(e -> eventOrder.add("childError"))
		    )
		    .doOnEach(sig -> eventOrder.add("parentEach" + sig))
		    .doOnSuccess(v -> eventOrder.add("parentSuccess"))
		    .doOnError(e -> eventOrder.add("parentError"))
		    .onErrorReturn("boom expected")
		    .block();

		assertThat(eventOrder).containsExactly(
				"childEachdoOnEach_onNext(child)",
				"childEachonError(java.lang.IllegalStateException: boomChild)",
				"childError",
				"parentEachonError(java.lang.IllegalStateException: boomChild)",
				"parentError");
	}

	@Test
	public void errorDuringCompleteSignalInMonoOnNextFused() {
		CopyOnWriteArrayList<String> eventOrder = new CopyOnWriteArrayList<>();

		Mono.fromSupplier(() -> "parent")
		    .flatMap(ignored -> Mono.fromSupplier(() -> "child")
		                            .doOnEach(sig -> {
			                            eventOrder.add("childEach" + sig);
			                            if (sig.isOnNext()) {
				                            throw new IllegalStateException("boomChild");
			                            }
		                            })
		                            .doOnSuccess(v -> eventOrder.add("childSuccess"))
		                            .doOnError(e -> eventOrder.add("childError"))
		    )
		    .doOnEach(sig -> eventOrder.add("parentEach" + sig))
		    .doOnSuccess(v -> eventOrder.add("parentSuccess"))
		    .doOnError(e -> eventOrder.add("parentError"))
		    .onErrorReturn("boom expected")
		    .block();

		assertThat(eventOrder).containsExactly(
				"childEachdoOnEach_onNext(child)",
				"childEachonError(java.lang.IllegalStateException: boomChild)",
				"childError",
				"parentEachonError(java.lang.IllegalStateException: boomChild)",
				"parentError");
	}

	@Test
	public void monoOnNextDoesntTriggerCompleteTwice() {
		AtomicInteger completeHandlerCount = new AtomicInteger();

		Mono.just("foo")
		    .hide()
		    .doOnEach(sig -> {
		    	if (sig.isOnComplete()) {
		    		completeHandlerCount.incrementAndGet();
			    }
		    })
		    .block();

		assertThat(completeHandlerCount).hasValue(1);
	}

	@Test
	public void monoOnNextDoesntTriggerCompleteTwiceFused() {
		AtomicInteger completeHandlerCount = new AtomicInteger();

		Mono.just("foo")
		    .doOnEach(sig -> {
		    	if (sig.isOnComplete()) {
		    		completeHandlerCount.incrementAndGet();
			    }
		    })
		    .block();

		assertThat(completeHandlerCount).hasValue(1);
	}

	@Test
	public void errorInCompleteHandlingTriggersErrorHandling() {
		AtomicInteger errorHandlerCount = new AtomicInteger();

		StepVerifier.create(
				Mono.just("foo")
				    .hide()
				    .doOnEach(sig -> {
					    if (sig.isOnComplete()) {
						    throw new IllegalStateException("boom");
					    }
					    if (sig.isOnError()) {
						    errorHandlerCount.incrementAndGet();
					    }
				    })
		)
		            .verifyErrorMessage("boom");

		assertThat(errorHandlerCount).as("error handler invoked on top on complete").hasValue(1);
	}

	@Test
	public void errorInCompleteHandlingTriggersErrorHandlingFused() {
		AtomicInteger errorHandlerCount = new AtomicInteger();

		StepVerifier.create(
				Mono.just("foo")
				    .doOnEach(sig -> {
					    if (sig.isOnComplete()) {
						    throw new IllegalStateException("boom");
					    }
					    if (sig.isOnError()) {
						    errorHandlerCount.incrementAndGet();
					    }
				    })
		)
		            .verifyErrorMessage("boom");

		assertThat(errorHandlerCount).as("error handler invoked on top on complete").hasValue(1);
	}

	@Test
	public void scanOperator(){
		final MonoDoOnEach<String> test = new MonoDoOnEach<>(Mono.just("foo"), s -> { });

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		final MonoDoOnEachFuseable<String> test = new MonoDoOnEachFuseable<>(Mono.just("foo"), s -> { });

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
