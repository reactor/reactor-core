package reactor.core.publisher;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIIII_Result;
import org.openjdk.jcstress.infra.results.IIII_Result;

import reactor.core.CoreSubscriber;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class MonoDelayUntilStressTest {

	@JCStressTest
	@Outcome(id = {"1, 1, 0, 0, 0"}, expect = ACCEPTABLE, desc = "No error dropped, composite error delivered. Cancel signal is late in that case")
	@Outcome(id = {"1, 1, 1, 1, 0", "1, 1, 0, 1, 0"}, expect = ACCEPTABLE, desc = "Main error possibly dropped, inner or composite error delivered. Main is cancelled. Cancel signal is late")
	@Outcome(id = {"1, 1, 1, 0, 1", "1, 1, 0, 0, 1"}, expect = ACCEPTABLE, desc = "Inner error possibly dropped, main or composite error delivered. Inner is cancelled. Cancel signal is late")
	@Outcome(id = {"1, 0, 1, 1, 1", "1, 0, 2, 1, 1"}, expect = ACCEPTABLE, desc = "No error delivered. One composite error delivered or both errors are dropped. Cancel signal is propagated to both")
	@State
	public static class InnerOnErrorAndOuterOnErrorAndCancelStressTest {

		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(1L);

		StressSubscription<Integer> subscriptionOuter;
		StressSubscription<Integer> subscriptionInner;

		{
			new Mono<Integer>() {
				@Override
				public void subscribe(CoreSubscriber<? super Integer> actual) {
					subscriptionOuter = new StressSubscription<>(actual);
					actual.onSubscribe(subscriptionOuter);
					actual.onNext(1);
				}
			}
			.delayUntil(__ -> new Mono<Integer>() {
				@Override
				public void subscribe(CoreSubscriber<? super Integer> actual) {
					subscriptionInner = new StressSubscription<>(actual);
					actual.onSubscribe(subscriptionInner);
				}
			})
			.subscribe(subscriber);
		}

		@Actor
		public void errorOuter() {
			subscriptionOuter.actual.onError(new RuntimeException("test1"));
		}

		@Actor
		public void errorInner() {
			subscriptionInner.actual.onError(new RuntimeException("test2"));
		}

		@Actor
		public void cancelFromActual() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(IIIII_Result r) {
			r.r1 = subscriber.onNextDiscarded.get();
			r.r2 = subscriber.onErrorCalls.get();
			r.r3 = subscriber.droppedErrors.size();
			r.r4 = subscriptionOuter.cancelled.get() ? 1 : 0;
			r.r5 = subscriptionInner.cancelled.get() ? 1 : 0;
		}
	}

	@JCStressTest
	@Outcome(id = {"1, 0, 1, 1"}, expect = ACCEPTABLE, desc = "Value discarded. Subscriptions cancelled")
	@Outcome(id = {"0, 1, 0, 0"}, expect = ACCEPTABLE, desc = "Value delivered. Cancel signal is late")
	@State
	public static class CompleteVsCancelStressTest {

		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(1L);

		StressSubscription<Integer> subscriptionOuter;
		StressSubscription<Integer> subscriptionInner;

		{
			new Mono<Integer>() {
				@Override
				public void subscribe(CoreSubscriber<? super Integer> actual) {
					subscriptionOuter = new StressSubscription<>(actual);
					actual.onSubscribe(subscriptionOuter);
					actual.onNext(1);
				}
			}
					.delayUntil(__ -> new Mono<Integer>() {
						@Override
						public void subscribe(CoreSubscriber<? super Integer> actual) {
							subscriptionInner = new StressSubscription<>(actual);
							actual.onSubscribe(subscriptionInner);
						}
					})
					.subscribe(subscriber);
		}

		@Actor
		public void completeOuter() {
			subscriptionOuter.actual.onComplete();
		}

		@Actor
		public void completeInner() {
			subscriptionInner.actual.onComplete();
		}

		@Actor
		public void cancelFromActual() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(IIII_Result r) {
			r.r1 = subscriber.onNextDiscarded.get();
			r.r2 = subscriber.onNextCalls.get();
			r.r3 = subscriptionOuter.cancelled.get() ? 1 : 0;
			r.r4 = subscriptionInner.cancelled.get() ? 1 : 0;
		}
	}



	@JCStressTest
	@Outcome(id = {"1, 0, 1, 1"}, expect = ACCEPTABLE, desc = "Value discarded. Subscriptions cancelled")
	@State
	public static class OnNextVsCancelStressTest {

		final StressSubscriber<Integer> subscriber = new StressSubscriber<Integer>(1L);

		StressSubscription<Integer> subscriptionOuter;
		StressSubscription<Integer> subscriptionInner;

		{
			new Mono<Integer>() {
				@Override
				public void subscribe(CoreSubscriber<? super Integer> actual) {
					subscriptionOuter = new StressSubscription<>(actual);
					actual.onSubscribe(subscriptionOuter);
				}
			}
					.delayUntil(__ -> new Mono<Integer>() {
						@Override
						public void subscribe(CoreSubscriber<? super Integer> actual) {
							subscriptionInner = new StressSubscription<>(actual);
							actual.onSubscribe(subscriptionInner);
						}
					})
					.subscribe(subscriber);
		}

		@Actor
		public void nextOuter() {
			subscriptionOuter.actual.onNext(1);
		}

		@Actor
		public void cancelFromActual() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(IIII_Result r) {
			r.r1 = subscriber.onNextDiscarded.get();
			r.r2 = subscriber.onNextCalls.get();
			r.r3 = subscriptionOuter.cancelled.get() ? 1 : 0;
			r.r4 = subscriptionInner.cancelled.get() ? 1 : 0;
		}
	}
}
