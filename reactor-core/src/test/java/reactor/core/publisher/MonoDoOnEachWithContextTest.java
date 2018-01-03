/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MonoDoOnEachWithContextTest {

	@Test
	public void nullSource() {
		Assertions.assertThatNullPointerException()
		          .isThrownBy(() -> new MonoDoOnEachWithContext<>(null, (s, c) -> {}))
		          .withMessage(null);
	}

	@Test
	public void nullBiConsumer() {
		Assertions.assertThatNullPointerException()
		          .isThrownBy(() -> new MonoDoOnEachWithContext<>(Mono.just("foo"),
				          (BiConsumer) null))
		          .withMessage("onSignalAndContext");
	}

	@Test
	public void usesFluxDoOnEachSubscriber() {
		@SuppressWarnings("unchecked")
		ArgumentCaptor<CoreSubscriber<String>> argumentCaptor =
				ArgumentCaptor.forClass(CoreSubscriber.class);
		@SuppressWarnings("unchecked")
		Mono<String> source = Mockito.mock(Mono.class);

		final MonoDoOnEachWithContext<String> test =
				new MonoDoOnEachWithContext<>(source, (s, c) -> { });

		test.subscribe();
		Mockito.verify(source).subscribe(argumentCaptor.capture());

		assertThat(argumentCaptor.getValue()).isInstanceOf(FluxDoOnEach.DoOnEachSubscriber.class);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger onNext = new AtomicInteger();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();

		Mono.just(1)
		    .hide()
		    .doWithContext((s, c) -> {
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

		assertThat(onNext.get()).isEqualTo(1);
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
	public void biConsumerError() {
		LongAdder state = new LongAdder();
		Throwable err = new Exception("test");

		StepVerifier.create(
				Mono.just(1)
				    .doWithContext((s,c) -> {
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
	public void biConsumerBubbleError() {
		LongAdder state = new LongAdder();
		Throwable err = new Exception("test");

		assertThatThrownBy(() ->
				StepVerifier.create(
						Mono.just(1)
						    .doWithContext((s,c) -> {
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
	public void nextCompleteWithContext() {
		List<Tuple2<Signal, Context>> signalsAndContext = new ArrayList<>();
		Mono.just(1)
		    .doWithContext((s, c) -> signalsAndContext.add(Tuples.of(s, c)))
		    .subscriberContext(Context.of("foo", "bar"))
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
	public void nextErrorWithContext() {
		List<Tuple2<Signal, Context>> signalsAndContext = new ArrayList<>();
		Mono.just(0)
		    .map(i -> 10 / i)
		    .doWithContext((s,c) -> signalsAndContext.add(Tuples.of(s,c)))
		    .subscriberContext(Context.of("foo", "bar"))
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
}