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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators.CancelledSubscription;
import reactor.core.publisher.Operators.DeferredSubscription;
import reactor.core.publisher.Operators.EmptySubscription;
import reactor.core.publisher.Operators.MonoSubscriber;
import reactor.core.publisher.Operators.MultiSubscriptionSubscriber;
import reactor.core.publisher.Operators.ScalarSubscription;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class OperatorsTest {

	volatile long testRequest;
	static final AtomicLongFieldUpdater<OperatorsTest> TEST_REQUEST =
			AtomicLongFieldUpdater.newUpdater(OperatorsTest.class, "testRequest");


	@Test
	public void addAndGetAtomicField() {
		TEST_REQUEST.set(this, 0L);

		RaceTestUtils.race(0L,
				s -> Operators.addCap(TEST_REQUEST, this, 1),
				a -> a >= 100_000L,
				(a, b) -> a == b
		);

		TEST_REQUEST.set(this, 0L);

		assertThat(Operators.addCap(TEST_REQUEST, this, -1_000_000L))
				.isEqualTo(0);

		TEST_REQUEST.set(this, 1L);

		assertThat(Operators.addCap(TEST_REQUEST, this, Long.MAX_VALUE))
				.isEqualTo(1L);

		assertThat(Operators.addCap(TEST_REQUEST, this, 0))
				.isEqualTo(Long.MAX_VALUE);
	}


	@Test
	public void addCap() {
		assertThat(Operators.addCap(1, 2)).isEqualTo(3);
		assertThat(Operators.addCap(1, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
		assertThat(Operators.addCap(0, -1)).isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void constructor(){
		assertThat(new Operators(){}).isNotNull();
	}

	@Test
	public void castAsQueueSubscription() {
		Fuseable.QueueSubscription<String> qs = new Fuseable.SynchronousSubscription<String>() {
			@Override
			public String poll() {
				return null;
			}

			@Override
			public int size() {
				return 0;
			}

			@Override
			public boolean isEmpty() {
				return false;
			}

			@Override
			public void clear() {

			}

			@Override
			public void request(long n) {

			}

			@Override
			public void cancel() {

			}
		};

		Subscription s = Operators.cancelledSubscription();

		assertThat(Operators.as(qs)).isEqualTo(qs);
		assertThat(Operators.as(s)).isNull();
	}

	@Test
	public void cancelledSubscription(){
		Operators.CancelledSubscription es =
				(Operators.CancelledSubscription)Operators.cancelledSubscription();

		assertThat((Object)es).isEqualTo(Operators.CancelledSubscription.INSTANCE);

		//Noop
		es.cancel();
		es.request(-1);
	}

	@Test
	public void noopFluxCancelled(){
		OperatorDisposables.DISPOSED.dispose(); //noop
	}

	@Test
	public void drainSubscriber() {
		AtomicBoolean requested = new AtomicBoolean();
		AtomicBoolean errored = new AtomicBoolean();
		try {
			Hooks.onErrorDropped(e -> {
				assertThat(Exceptions.isErrorCallbackNotImplemented(e)).isTrue();
				assertThat(e.getCause()).hasMessage("test");
				errored.set(true);
			});
			Flux.from(s -> {
				assertThat(s).isEqualTo(Operators.drainSubscriber());
				s.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
						assertThat(n).isEqualTo(Long.MAX_VALUE);
						requested.set(true);
					}

					@Override
					public void cancel() {

					}
				});
				s.onNext("ignored"); //dropped
				s.onComplete(); //dropped
				s.onError(new Exception("test"));
			})
			    .subscribe(Operators.drainSubscriber());

			assertThat(requested.get()).isTrue();
			assertThat(errored.get()).isTrue();
		}
		finally {
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	public void scanCancelledSubscription() {
		CancelledSubscription test = CancelledSubscription.INSTANCE;
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanDeferredSubscription() {
		DeferredSubscription test = new DeferredSubscription();
		test.s = Operators.emptySubscription();

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(test.s);
		test.requested = 123;
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanEmptySubscription() {
		EmptySubscription test = EmptySubscription.INSTANCE;
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanMonoSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, null, null, null);
		MonoSubscriber<Integer, Integer> test = new MonoSubscriber<>(actual);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.complete(4);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();

	}

	@Test
	public void scanMultiSubscriptionSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, null, null, null);
		MultiSubscriptionSubscriber<Integer, Integer> test = new MultiSubscriptionSubscriber<Integer, Integer>(actual) {
			@Override
			public void onNext(Integer t) {
			}
		};
		Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		test.request(34);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(34);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanScalarSubscription() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, null, null, null);
		ScalarSubscription<Integer> test = new ScalarSubscription<>(actual, 5);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.poll();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void onNextErrorModeLocalStrategy() {
		List<Object> nextDropped = new ArrayList<>();
		List<Object> errorDropped = new ArrayList<>();
		Hooks.onNextDropped(nextDropped::add);
		Hooks.onErrorDropped(errorDropped::add);
		Hooks.onNextError(OnNextFailureStrategy.STOP);

		Context c = Context.of(OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY, OnNextFailureStrategy.RESUME_DROP);
		Exception error = new IllegalStateException("boom");
		DeferredSubscription s = new Operators.DeferredSubscription();

		try {
			assertThat(s.isCancelled()).as("s initially cancelled").isFalse();

			Throwable e = Operators.onNextError("foo", error, c, s);
			assertThat(e).isNull();
			assertThat(nextDropped).containsExactly("foo");
			assertThat(errorDropped).containsExactly(error);
			assertThat(s.isCancelled()).as("s cancelled").isFalse();
		}
		finally {
			Hooks.resetOnNextDropped();
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextError();
		}
	}

	@Test
	public void pollErrorModeLocalStrategy() {
		List<Object> nextDropped = new ArrayList<>();
		List<Object> errorDropped = new ArrayList<>();
		Hooks.onNextDropped(nextDropped::add);
		Hooks.onErrorDropped(errorDropped::add);

		Context c = Context.of(OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY, OnNextFailureStrategy.RESUME_DROP);
		Exception error = new IllegalStateException("boom");
		try {
			assertThat(Hooks.onNextErrorHook).as("no global hook").isNull();

			RuntimeException e = Operators.onNextPollError("foo", error, c);
			assertThat(e).isNull();
			assertThat(nextDropped).containsExactly("foo");
			assertThat(errorDropped).containsExactly(error);
		}
		finally {
			Hooks.resetOnNextDropped();
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	public void onErrorDroppedLocal() {
		AtomicReference<Throwable> hookState = new AtomicReference<>();
		Consumer<Throwable> localHook = hookState::set;
		Context c = Context.of(Hooks.KEY_ON_ERROR_DROPPED, localHook);

		Operators.onErrorDropped(new IllegalArgumentException("boom"), c);

		assertThat(hookState.get()).isInstanceOf(IllegalArgumentException.class)
		                           .hasMessage("boom");
	}

	@Test
	public void onNextDroppedLocal() {
		AtomicReference<Object> hookState = new AtomicReference<>();
		Consumer<Object> localHook = hookState::set;
		Context c = Context.of(Hooks.KEY_ON_NEXT_DROPPED, localHook);

		Operators.onNextDropped("foo", c);

		assertThat(hookState.get()).isEqualTo("foo");
	}

	@Test
	public void onOperatorErrorLocal() {
		BiFunction<Throwable, Object, Throwable> localHook = (e, v) ->
				new IllegalStateException("boom_" + v, e);
		Context c = Context.of(Hooks.KEY_ON_OPERATOR_ERROR, localHook);

		IllegalArgumentException failure = new IllegalArgumentException("foo");

		final Throwable throwable = Operators.onOperatorError(null, failure,
				"foo", c);

		assertThat(throwable).isInstanceOf(IllegalStateException.class)
		                     .hasMessage("boom_foo")
		                     .hasCause(failure);
	}

	@Test
	public void onRejectedExecutionWithoutDataSignalDelegatesToErrorLocal() {
		BiFunction<Throwable, Object, Throwable> localHook = (e, v) ->
				new IllegalStateException("boom_" + v, e);
		Context c = Context.of(Hooks.KEY_ON_OPERATOR_ERROR, localHook);

		IllegalArgumentException failure = new IllegalArgumentException("foo");
		final Throwable throwable = Operators.onRejectedExecution(failure, null, null, null, c);

		assertThat(throwable).isInstanceOf(IllegalStateException.class)
		                     .hasMessage("boom_null")
		                     .hasNoSuppressedExceptions();
		assertThat(throwable.getCause()).isInstanceOf(RejectedExecutionException.class)
		                                .hasMessage("Scheduler unavailable")
		                                .hasCause(failure);
	}

	@Test
	public void onRejectedExecutionWithDataSignalDelegatesToErrorLocal() {
		BiFunction<Throwable, Object, Throwable> localHook = (e, v) ->
				new IllegalStateException("boom_" + v, e);
		Context c = Context.of(Hooks.KEY_ON_OPERATOR_ERROR, localHook);

		IllegalArgumentException failure = new IllegalArgumentException("foo");
		final Throwable throwable = Operators.onRejectedExecution(failure, null,
				null, "bar", c);

		assertThat(throwable).isInstanceOf(IllegalStateException.class)
		                     .hasMessage("boom_bar")
		                     .hasNoSuppressedExceptions();
		assertThat(throwable.getCause()).isInstanceOf(RejectedExecutionException.class)
		                                .hasMessage("Scheduler unavailable")
		                                .hasCause(failure);
	}

	@Test
	public void onRejectedExecutionLocalTakesPrecedenceOverOnOperatorError() {
		BiFunction<Throwable, Object, Throwable> localOperatorErrorHook = (e, v) ->
				new IllegalStateException("boom_" + v, e);

		BiFunction<Throwable, Object, Throwable> localReeHook = (e, v) ->
				new IllegalStateException("rejected_" + v, e);
		Context c = Context.of(
				Hooks.KEY_ON_OPERATOR_ERROR, localOperatorErrorHook,
				Hooks.KEY_ON_REJECTED_EXECUTION, localReeHook);

		IllegalArgumentException failure = new IllegalArgumentException("foo");
		final Throwable throwable = Operators.onRejectedExecution(failure, null,
				null, "bar", c);

		assertThat(throwable).isInstanceOf(IllegalStateException.class)
		                     .hasMessage("rejected_bar")
		                     .hasNoSuppressedExceptions();
		assertThat(throwable.getCause()).isInstanceOf(RejectedExecutionException.class)
		                                .hasMessage("Scheduler unavailable")
		                                .hasCause(failure);
	}

	@Test
	public void testOnRejectedWithReactorRee() {
		Exception originalCause = new Exception("boom");
		RejectedExecutionException original = Exceptions.failWithRejected(originalCause);
		Exception suppressed = new Exception("suppressed");

		RuntimeException test = Operators.onRejectedExecution(original,
				null, suppressed, null, Context.empty());

		assertThat(test)
				.isSameAs(original)
				.hasSuppressedException(suppressed);
	}

	@Test
	public void testOnRejectedWithOutsideRee() {
		RejectedExecutionException original = new RejectedExecutionException("outside");
		Exception suppressed = new Exception("suppressed");

		RuntimeException test = Operators.onRejectedExecution(original,
				null, suppressed, null, Context.empty());

		assertThat(test)
				.isNotSameAs(original)
				.isInstanceOf(RejectedExecutionException.class)
				.hasMessage("Scheduler unavailable")
				.hasCause(original)
				.hasSuppressedException(suppressed);
	}

	@Test
	public void unboundedOrPrefetch() {
		assertThat(Operators.unboundedOrPrefetch(10))
				.as("bounded")
				.isEqualTo(10L);
		assertThat(Operators.unboundedOrPrefetch(Integer.MAX_VALUE))
				.as("unbounded")
				.isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void unboundedOrLimit() {
		assertThat(Operators.unboundedOrLimit(100))
				.as("prefetch - (prefetch >> 2)")
				.isEqualTo(75);

		assertThat(Operators.unboundedOrLimit(Integer.MAX_VALUE))
				.as("unbounded")
				.isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void unboundedOrLimitLowTide() {
		assertThat(Operators.unboundedOrLimit(100, 100))
				.as("same lowTide")
				.isEqualTo(75);

		assertThat(Operators.unboundedOrLimit(100, 110))
				.as("too big lowTide")
				.isEqualTo(75);

		assertThat(Operators.unboundedOrLimit(Integer.MAX_VALUE, Integer.MAX_VALUE))
				.as("MAX_VALUE and same lowTide")
				.isEqualTo(Integer.MAX_VALUE);

		assertThat(Operators.unboundedOrLimit(100, 20))
				.as("smaller lowTide")
				.isEqualTo(20);

		assertThat(Operators.unboundedOrLimit(Integer.MAX_VALUE, 110))
				.as("smaller lowTide and MAX_VALUE")
				.isEqualTo(Integer.MAX_VALUE);

		assertThat(Operators.unboundedOrLimit(100, 0))
				.as("0 lowTide and 100")
				.isEqualTo(100);

		assertThat(Operators.unboundedOrLimit(Integer.MAX_VALUE, 0))
				.as("0 lowTide and MAX_VALUE")
				.isEqualTo(Integer.MAX_VALUE);

		assertThat(Operators.unboundedOrLimit(100, -1))
				.as("-1 lowTide and 100")
				.isEqualTo(100);

		assertThat(Operators.unboundedOrLimit(Integer.MAX_VALUE, -1))
				.as("-1 lowTide and MAX_VALUE")
				.isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void onNextFailureWithStrategyMatchingDoesntCancel() {
		Context context = Context.of(OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY, new OnNextFailureStrategy() {
			@Override
			public boolean test(Throwable error, @Nullable Object value) {
				return true;
			}

			@Nullable
			@Override
			public Throwable process(Throwable error, @Nullable Object value,
					Context context) {
				return null;
			}
		});

		Operators.DeferredSubscription s = new Operators.DeferredSubscription();
		Throwable t = Operators.onNextError("foo", new NullPointerException("bar"), context, s);

		assertThat(t).as("exception processed").isNull();
		assertThat(s.isCancelled()).as("subscription cancelled").isFalse();
	}

	@Test
	public void onNextFailureWithStrategyNotMatchingDoesCancel() {
		Context context = Context.of(OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY, new OnNextFailureStrategy() {
			@Override
			public boolean test(Throwable error, @Nullable Object value) {
				return false;
			}

			@Override
			public Throwable process(Throwable error, @Nullable Object value,
					Context context) {
				return error;
			}
		});

		Operators.DeferredSubscription s = new Operators.DeferredSubscription();
		Throwable t = Operators.onNextError("foo", new NullPointerException("bar"), context, s);

		assertThat(t).as("exception processed")
		             .isNotNull()
		             .isInstanceOf(NullPointerException.class)
		             .hasNoSuppressedExceptions()
		             .hasNoCause();
		assertThat(s.isCancelled()).as("subscription cancelled").isTrue();
	}

	@Test
	public void onNextFailureWithStrategyMatchingButNotNullDoesCancel() {
		Context context = Context.of(OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY, new OnNextFailureStrategy() {
			@Override
			public boolean test(Throwable error, @Nullable Object value) {
				return true;
			}

			@Override
			public Throwable process(Throwable error, @Nullable Object value,
					Context context) {
				return error;
			}
		});

		Operators.DeferredSubscription s = new Operators.DeferredSubscription();
		Throwable t = Operators.onNextError("foo", new NullPointerException("bar"), context, s);

		assertThat(t).as("exception processed")
		             .isNotNull()
		             .isInstanceOf(NullPointerException.class)
		             .hasNoSuppressedExceptions()
		             .hasNoCause();
		assertThat(s.isCancelled()).as("subscription cancelled").isTrue();
	}

	@Test
	public void liftVsLiftPublisher() {
		Publisher<Object> notScannable = new Flux<Object>() {
			@Override
			public void subscribe(CoreSubscriber<? super Object> actual) { }
		};

		AtomicReference<Scannable> scannableRef = new AtomicReference<>();
		AtomicReference<Publisher> rawRef = new AtomicReference<>();

		@SuppressWarnings("unchecked")
		Function<Publisher<Object>, Publisher<Object>> lift = (Function<Publisher<Object>, Publisher<Object>>)
				Operators.lift((sc, sub) -> {
					scannableRef.set(sc);
					return sub;
				});

		@SuppressWarnings("unchecked")
		Function<Publisher<Object>, Publisher<Object>> liftPublisher = (Function<Publisher<Object>, Publisher<Object>>)
				Operators.liftPublisher((pub, sub) -> {
					rawRef.set(pub);
					return sub;
				});

		Publisher<Object> lifted = lift.apply(notScannable);
		Publisher<Object> liftedRaw = liftPublisher.apply(notScannable);

		assertThat(scannableRef).hasValue(null);
		assertThat(rawRef).hasValue(null);

		lifted.subscribe(new BaseSubscriber<Object>() {});
		liftedRaw.subscribe(new BaseSubscriber<Object>() {});

		assertThat(scannableRef.get())
				.isNotNull()
				.isNotSameAs(notScannable)
				.matches(s -> !s.isScanAvailable(), "not scannable");
		assertThat(rawRef.get())
				.isNotNull()
				.isSameAs(notScannable);
	}

	@Test
	public void liftVsLiftRawWithPredicate() {
		Publisher<Object> notScannable = new Flux<Object>() {
			@Override
			public void subscribe(CoreSubscriber<? super Object> actual) { }
		};

		AtomicReference<Scannable> scannableRef = new AtomicReference<>();
		AtomicReference<Scannable> scannableFilterRef = new AtomicReference<>();
		AtomicReference<Publisher> rawRef = new AtomicReference<>();
		AtomicReference<Publisher> rawFilterRef = new AtomicReference<>();

		@SuppressWarnings("unchecked")
		Function<Publisher<Object>, Publisher<Object>> lift = (Function<Publisher<Object>, Publisher<Object>>)
				Operators.lift(sc -> {
							scannableFilterRef.set(sc);
							return true;
						},
						(sc, sub) -> {
							scannableRef.set(sc);
							return sub;
						});

		@SuppressWarnings("unchecked")
		Function<Publisher<Object>, Publisher<Object>> liftRaw = (Function<Publisher<Object>, Publisher<Object>>)
				Operators.liftPublisher(pub -> {
							rawFilterRef.set(pub);
							return true;
						},
						(pub, sub) -> {
							rawRef.set(pub);
							return sub;
						});

		Publisher<Object> lifted = lift.apply(notScannable);
		Publisher<Object> liftedRaw = liftRaw.apply(notScannable);

		assertThat(scannableRef).hasValue(null);
		assertThat(scannableFilterRef).doesNotHaveValue(null);
		assertThat(rawRef).hasValue(null);
		assertThat(rawFilterRef).doesNotHaveValue(null);

		lifted.subscribe(new BaseSubscriber<Object>() {});
		liftedRaw.subscribe(new BaseSubscriber<Object>() {});

		assertThat(scannableRef.get())
				.isNotNull()
				.isNotSameAs(notScannable)
				.matches(s -> !s.isScanAvailable(), "not scannable")
				.isSameAs(scannableFilterRef.get());

		assertThat(rawRef.get())
				.isNotNull()
				.isSameAs(notScannable)
				.isSameAs(rawFilterRef.get());
	}

	@Test
	public void discardAdapterRejectsNull() {
		assertThatNullPointerException().isThrownBy(() -> Operators.discardLocalAdapter(null, obj -> {}))
		                                .as("type null check")
		                                .withMessage("onDiscard must be based on a type");
		assertThatNullPointerException().isThrownBy(() -> Operators.discardLocalAdapter(String.class, null))
		                                .as("discardHook null check")
		                                .withMessage("onDiscard must be provided a discardHook Consumer");
	}

	@Test
	public void discardAdapterIsAdditive() {
		List<String> discardOrder = Collections.synchronizedList(new ArrayList<>(2));

		Function<Context, Context> first = Operators.discardLocalAdapter(Number.class, i -> discardOrder.add("FIRST"));
		Function<Context, Context> second = Operators.discardLocalAdapter(Integer.class, i -> discardOrder.add("SECOND"));

		Context ctx = first.apply(second.apply(Context.empty()));
		Consumer<Object> test = ctx.getOrDefault(Hooks.KEY_ON_DISCARD, o -> {});

		assertThat(test).isNotNull();

		test.accept(1);
		assertThat(discardOrder).as("consumers were combined").containsExactly("FIRST", "SECOND");
	}

	@Test
	public void convertNonConditionalToConditionalSubscriberTest() {
		Object elementToSend = new Object();
		ArrayList<Object> captured = new ArrayList<>();
		BaseSubscriber<Object> actual = new BaseSubscriber<Object>() {
			@Override
			protected void hookOnNext(Object value) {
				captured.add(value);
			}
		};
		Fuseable.ConditionalSubscriber<? super Object> conditionalSubscriber =
			Operators.toConditionalSubscriber(actual);

		Assertions.assertThat(conditionalSubscriber).isNotEqualTo(actual);
		Assertions.assertThat(conditionalSubscriber.tryOnNext(elementToSend)).isTrue();
		Assertions.assertThat(captured).containsExactly(elementToSend);
	}

	@Test
	public void convertConditionalToConditionalShouldReturnTheSameInstance() {
		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<String> original = Mockito.mock(Fuseable.ConditionalSubscriber.class);

		Assertions.assertThat(Operators.toConditionalSubscriber(original))
		          .isSameAs(original);
	}
}
