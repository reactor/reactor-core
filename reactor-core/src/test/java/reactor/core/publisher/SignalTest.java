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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.test.StepVerifier;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class SignalTest {

	Exception e = new Exception("test");

	@Test
	public void completeWithoutContextIsSingleton() {
		assertThat(Signal.complete()).isSameAs(Signal.complete());
	}

	@Test
	public void completeWithContextCreatesNewInstances() {
		Context context = Context.of("foo", "bar");
		assertThat(Signal.complete(context))
				.isNotSameAs(Signal.complete(context))
				.isNotSameAs(Signal.complete())
				.isEqualTo(Signal.complete())
				.isEqualTo(Signal.complete(context));
	}

	@Test
	public void completeStateWithContext(){
		Context context = Context.of("foo", "bar");
		Signal<Integer> s = Signal.complete(context);

		assertThat(s.getContextView().isEmpty()).as("has context").isFalse();

		assertThat(s.isOnComplete()).isTrue();
		assertThat(s.isOnSubscribe()).isFalse();
		assertThat(s.hasError()).isFalse();
		assertThat(s.hasValue()).isFalse();

		assertThat(s).isEqualTo(Signal.complete());
		assertThat(s).isNotEqualTo(Signal.error(e));
		assertThat(s).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()));
		assertThat(s).isNotEqualTo(Signal.next(1));
		assertThat(s.hashCode()).isEqualTo(Signal.complete().hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.error(e).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.next(1).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()).hashCode());

		assertThat(Signal.isComplete(s)).isTrue();
		assertThat(Signal.isError(s)).isFalse();

		assertThat(s.getType()).isEqualTo(SignalType.ON_COMPLETE);
		assertThat(s.toString()).contains("onComplete");

		StepVerifier.create(Flux.<Integer>from(sub -> {
			sub.onSubscribe(Operators.emptySubscription());
			s.accept(sub);
		}))
		            .verifyComplete();
	}

	@Test
	public void errorStateWithContext(){
		Context context = Context.of("foo", "bar");
		Signal<Integer> s = Signal.error(e, context);

		assertThat(s.getContextView().isEmpty()).as("has context").isFalse();

		assertThat(s.isOnComplete()).isFalse();
		assertThat(s.isOnSubscribe()).isFalse();
		assertThat(s.hasError()).isTrue();
		assertThat(s.hasValue()).isFalse();

		assertThat(s).isEqualTo(Signal.error(e));
		assertThat(s).isNotEqualTo(Signal.error(new Exception("test2")));
		assertThat(s).isNotEqualTo(Signal.complete());
		assertThat(s).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()));
		assertThat(s).isNotEqualTo(Signal.next(1));
		assertThat(s.hashCode()).isEqualTo(Signal.error(e).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.error(new Exception("test2")).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.complete().hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.next(1).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()).hashCode());

		assertThat(Signal.isComplete(s)).isFalse();
		assertThat(Signal.isError(s)).isTrue();
		assertThat(s.getThrowable()).isEqualTo(e);

		assertThat(s.getType()).isEqualTo(SignalType.ON_ERROR);
		assertThat(s.toString()).contains("onError");

		StepVerifier.create(Flux.<Integer>from(sub -> {
			sub.onSubscribe(Operators.emptySubscription());
			s.accept(sub);
		}))
	                .verifyErrorMessage("test");
	}

	@Test
	public void nextStateWithContext(){
		Context context = Context.of("foo", "bar");
		Signal<Integer> s = Signal.next(1, context);

		assertThat(s.getContextView().isEmpty()).as("has context").isFalse();

		assertThat(s.isOnComplete()).isFalse();
		assertThat(s.isOnSubscribe()).isFalse();
		assertThat(s.hasError()).isFalse();
		assertThat(s.hasValue()).isTrue();

		assertThat(s).isEqualTo(Signal.next(1));
		assertThat(s).isNotEqualTo(Signal.next(2));
		assertThat(s).isNotEqualTo(Signal.error(e));
		assertThat(s).isNotEqualTo(Signal.complete());
		assertThat(s).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()));
		assertThat(s.hashCode()).isEqualTo(Signal.next(1).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.next(2).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.error(e).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.complete().hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()).hashCode());

		assertThat(Signal.isComplete(s)).isFalse();
		assertThat(Signal.isError(s)).isFalse();
		assertThat(s.get()).isEqualTo(1);

		assertThat(s.getType()).isEqualTo(SignalType.ON_NEXT);
		assertThat(s.toString()).contains("onNext(1)");

		StepVerifier.create(Flux.<Integer>from(sub -> {
			sub.onSubscribe(Operators.emptySubscription());
			s.accept(sub);
		}))
		            .expectNext(1)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void subscribeStateWithContext(){
		Context context = Context.of("foo", "bar");
		Signal<Integer> s = Signal.subscribe(Operators.emptySubscription(), context);

		assertThat(s.getContextView().isEmpty()).as("has context").isFalse();

		assertThat(s.isOnComplete()).isFalse();
		assertThat(s.isOnSubscribe()).isTrue();
		assertThat(s.hasError()).isFalse();
		assertThat(s.hasValue()).isFalse();

		assertThat(s).isEqualTo(Signal.subscribe(Operators.emptySubscription()));
		assertThat(s).isNotEqualTo(Signal.subscribe(Operators.cancelledSubscription()));
		assertThat(s).isNotEqualTo(Signal.next(1));
		assertThat(s).isNotEqualTo(Signal.error(e));
		assertThat(s).isNotEqualTo(Signal.complete());
		assertThat(s.hashCode()).isEqualTo(Signal.subscribe(Operators.emptySubscription()).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.subscribe(Operators.cancelledSubscription()).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.next(1).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.error(e).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.complete().hashCode());

		assertThat(Signal.isComplete(s)).isFalse();
		assertThat(Signal.isError(s)).isFalse();
		assertThat(s.getSubscription()).isEqualTo(Operators.emptySubscription());

		assertThat(s.getType()).isEqualTo(SignalType.ON_SUBSCRIBE);
		assertThat(s.toString()).contains("onSubscribe");
		StepVerifier.create(Flux.<Integer>from(s::accept))
		            .expectSubscription()
		            .thenCancel()
		            .verify();
	}
	@Test
	public void completeState(){
		Signal<Integer> s = Signal.complete();

		assertThat(s.isOnComplete()).isTrue();
		assertThat(s.isOnSubscribe()).isFalse();
		assertThat(s.hasError()).isFalse();
		assertThat(s.hasValue()).isFalse();

		assertThat(s).isEqualTo(Signal.complete());
		assertThat(s).isNotEqualTo(Signal.error(e));
		assertThat(s).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()));
		assertThat(s).isNotEqualTo(Signal.next(1));
		assertThat(s.hashCode()).isEqualTo(Signal.complete().hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.error(e).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.next(1).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()).hashCode());

		assertThat(Signal.isComplete(s)).isTrue();
		assertThat(Signal.isError(s)).isFalse();

		assertThat(s.getType()).isEqualTo(SignalType.ON_COMPLETE);
		assertThat(s.toString()).contains("onComplete");

		StepVerifier.create(Flux.<Integer>from(sub -> {
			sub.onSubscribe(Operators.emptySubscription());
			s.accept(sub);
		}))
		            .verifyComplete();
	}

	@Test
	public void errorState(){
		Signal<Integer> s = Signal.error(e);

		assertThat(s.isOnComplete()).isFalse();
		assertThat(s.isOnSubscribe()).isFalse();
		assertThat(s.hasError()).isTrue();
		assertThat(s.hasValue()).isFalse();

		assertThat(s).isEqualTo(Signal.error(e));
		assertThat(s).isNotEqualTo(Signal.error(new Exception("test2")));
		assertThat(s).isNotEqualTo(Signal.complete());
		assertThat(s).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()));
		assertThat(s).isNotEqualTo(Signal.next(1));
		assertThat(s.hashCode()).isEqualTo(Signal.error(e).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.error(new Exception("test2")).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.complete().hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.next(1).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()).hashCode());

		assertThat(Signal.isComplete(s)).isFalse();
		assertThat(Signal.isError(s)).isTrue();
		assertThat(s.getThrowable()).isEqualTo(e);

		assertThat(s.getType()).isEqualTo(SignalType.ON_ERROR);
		assertThat(s.toString()).contains("onError");

		StepVerifier.create(Flux.<Integer>from(sub -> {
			sub.onSubscribe(Operators.emptySubscription());
			s.accept(sub);
		}))
	                .verifyErrorMessage("test");
	}

	@Test
	public void nextState(){
		Signal<Integer> s = Signal.next(1);

		assertThat(s.isOnComplete()).isFalse();
		assertThat(s.isOnSubscribe()).isFalse();
		assertThat(s.hasError()).isFalse();
		assertThat(s.hasValue()).isTrue();

		assertThat(s).isEqualTo(Signal.next(1));
		assertThat(s).isNotEqualTo(Signal.next(2));
		assertThat(s).isNotEqualTo(Signal.error(e));
		assertThat(s).isNotEqualTo(Signal.complete());
		assertThat(s).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()));
		assertThat(s.hashCode()).isEqualTo(Signal.next(1).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.next(2).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.error(e).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.complete().hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.subscribe(Operators.emptySubscription()).hashCode());

		assertThat(Signal.isComplete(s)).isFalse();
		assertThat(Signal.isError(s)).isFalse();
		assertThat(s.get()).isEqualTo(1);

		assertThat(s.getType()).isEqualTo(SignalType.ON_NEXT);
		assertThat(s.toString()).contains("onNext(1)");

		StepVerifier.create(Flux.<Integer>from(sub -> {
			sub.onSubscribe(Operators.emptySubscription());
			s.accept(sub);
		}))
		            .expectNext(1)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void subscribeState(){
		Signal<Integer> s = Signal.subscribe(Operators.emptySubscription());

		assertThat(s.isOnComplete()).isFalse();
		assertThat(s.isOnSubscribe()).isTrue();
		assertThat(s.hasError()).isFalse();
		assertThat(s.hasValue()).isFalse();

		assertThat(s).isEqualTo(Signal.subscribe(Operators.emptySubscription()));
		assertThat(s).isNotEqualTo(Signal.subscribe(Operators.cancelledSubscription()));
		assertThat(s).isNotEqualTo(Signal.next(1));
		assertThat(s).isNotEqualTo(Signal.error(e));
		assertThat(s).isNotEqualTo(Signal.complete());
		assertThat(s.hashCode()).isEqualTo(Signal.subscribe(Operators.emptySubscription()).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.subscribe(Operators.cancelledSubscription()).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.next(1).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.error(e).hashCode());
		assertThat(s.hashCode()).isNotEqualTo(Signal.complete().hashCode());

		assertThat(Signal.isComplete(s)).isFalse();
		assertThat(Signal.isError(s)).isFalse();
		assertThat(s.getSubscription()).isEqualTo(Operators.emptySubscription());

		assertThat(s.getType()).isEqualTo(SignalType.ON_SUBSCRIBE);
		assertThat(s.toString()).contains("onSubscribe");
		StepVerifier.create(Flux.<Integer>from(s::accept))
		            .expectSubscription()
		            .thenCancel()
		            .verify();
	}

	@Test
	public void unmatchingSignal(){
		assertThat(Signal.complete().equals(null)).isFalse();
		assertThat(Signal.isError(1)).isFalse();
		assertThat(Signal.complete().equals(Signal.next(1))).isFalse();
		assertThat(Signal.complete().equals(1)).isFalse();
		assertThat(Signal.complete().equals(new Signal<Object>() {
			@Override
			public Throwable getThrowable() {
				return null;
			}

			@Override
			public Subscription getSubscription() {
				return null;
			}

			@Override
			public Object get() {
				return null;
			}

			@Override
			public ContextView getContextView() {
				return Context.empty();
			}

			@Override
			public SignalType getType() {
				return SignalType.AFTER_TERMINATE;
			}
		})).isFalse();
	}

	@Test
	public void equalsIgnoresContext() {
		Signal<String> next1 = Signal.next("foo");
		Signal<String> next2 = Signal.next("foo", Context.of("bar", "baz"));

		assertThat(next1.getContextView().isEmpty()).as("next1 context empty").isTrue();
		assertThat(next2.getContextView().isEmpty()).as("next2 context not empty").isFalse();
		assertThat(next1).isEqualTo(next2);
	}
}
