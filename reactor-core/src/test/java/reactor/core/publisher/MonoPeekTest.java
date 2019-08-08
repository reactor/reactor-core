/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoPeekTest {

	@Test
	public void onMonoRejectedDoOnSuccessOrError() {
		Mono<String> mp = Mono.error(new Exception("test"));
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnSuccessOrError((s, f) -> ref.set(f))
		  .subscribe();

		assertThat(ref.get()).hasMessage("test");
	}

	@Test
	public void onMonoSuccessDoOnSuccessOrError() {
		Mono<String> mp = Mono.just("test");
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnSuccessOrError((s, f) -> ref.set(s))
		  .subscribe();

		assertThat(ref.get()).isEqualToIgnoringCase("test");
	}

	@Test
	public void onMonoRejectedDoOnTerminate() {
		Mono<String> mp = Mono.error(new Exception("test"));
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet)
		  .subscribe();

		assertThat(invoked.get()).isEqualTo(1);
	}

	@Test
	public void onMonoSuccessDoOnTerminate() {
		Mono<String> mp = Mono.just("test");
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet)
		  .subscribe();

		assertThat(invoked.get()).isEqualTo(1);
	}

	@Test
	public void onMonoSuccessDoOnSuccess() {
		Mono<String> mp = Mono.just("test");
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnSuccess(ref::set)
		  .subscribe();

		assertThat(ref.get()).isEqualToIgnoringCase("test");
	}

	@Test
	public void onMonoDoOnRequest() {
		Mono<String> mp = Mono.just("test");
		AtomicReference<Long> ref = new AtomicReference<>();

		StepVerifier.create(mp.doOnRequest(ref::set), 0)
		            .thenAwait()
		            .thenRequest(123)
		            .expectNext("test")
		            .verifyComplete();

		assertThat(ref.get()).isEqualTo(123);
	}

	@Test
	public void onMonoDoOnSubscribe() {
		Mono<String> mp = Mono.just("test");
		AtomicReference<Subscription> ref = new AtomicReference<>();

		StepVerifier.create(mp.doOnSubscribe(ref::set))
		            .expectNext("test")
		            .verifyComplete();

		assertThat(ref.get()).isNotNull();
	}

	@Test
	public void onMonoRejectedDoOnError() {
		Mono<String> mp = Mono.error(new Exception("test"));
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set)
		  .subscribe();

		assertThat(ref.get()).hasMessage("test");
	}

	final static class TestException extends Exception {

	}

	@Test
	public void onMonoRejectedDoOnErrorClazz() {
		Mono<String> mp = Mono.error(new TestException());
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(TestException.class, ref::set)
		  .subscribe();

		assertThat(ref.get()).isInstanceOf(TestException.class);
	}

	@Test
	public void onMonoRejectedDoOnErrorClazzNot() {
		Mono<String> mp = Mono.error(new TestException());
		AtomicReference<Throwable> ref = new AtomicReference<>();

		MonoProcessor<String> processor = mp.doOnError(RuntimeException.class, ref::set)
		                                    .toProcessor();
		processor.subscribe();
		assertThat(processor.getError()).isInstanceOf(TestException.class);

		assertThat(ref.get()).isNull();
	}

	@Test(expected = NullPointerException.class)
	public void onMonoSuccessNullDoOnSuccess() {
		Mono<String> mp = Mono.just("test");
		mp.doOnSuccess(null)
		  .subscribe();
	}

	@Test
	public void testErrorWithDoOnSuccess() {
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() ->
						Mono.error(new NullPointerException("boom"))
						    .doOnSuccess(aValue -> {})
						    .subscribe())
				.withCauseInstanceOf(NullPointerException.class)
				.matches(Exceptions::isErrorCallbackNotImplemented, "ErrorCallbackNotImplemented");
	}

	@Test
	public void macroFusionNormal() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();
		AtomicInteger requestHit = new AtomicInteger();
		AtomicInteger cancelHit = new AtomicInteger();
		AtomicInteger nextHit = new AtomicInteger();
		AtomicInteger errorHit = new AtomicInteger();
		AtomicInteger otherSignalHit = new AtomicInteger();


		final Mono<Integer> combiningWithNulls = Mono
				.just(1)
				.hide()
				.doOnSubscribe(subRef::set) //basis for first operator, all other handlers are null so far
				.doOnSubscribe(sub -> otherSignalHit.incrementAndGet()) //combine the subscribe handlers
				.doOnNext(v -> nextHit.incrementAndGet())
				.doOnError(e -> errorHit.incrementAndGet())
				.doOnTerminate(otherSignalHit::incrementAndGet)
				.doOnRequest(r -> requestHit.incrementAndGet())
				.doOnCancel(cancelHit::incrementAndGet);

		combiningWithNulls.as(StepVerifier::create)
		                  .expectNoFusionSupport()
		                  .expectNext(1)
		                  .verifyComplete();

		assertThat(otherSignalHit).as("sub/terminate").hasValue(2);
		assertThat(requestHit).as("request hit").hasValue(1);
		assertThat(cancelHit).as("cancel never hit").hasValue(0);
		assertThat(nextHit).as("onNext hit").hasValue(1);
		assertThat(errorHit).as("error never hit").hasValue(0);

		final Mono<Integer> combiningWithNonNulls =
				combiningWithNulls.doOnSubscribe(subRef::set) //basis for first operator, all other handlers are null so far
				                  .doOnSubscribe(sub -> otherSignalHit.incrementAndGet()) //combine the subscribe handlers
				                  .doOnNext(v -> nextHit.incrementAndGet())
				                  .doOnError(e -> errorHit.incrementAndGet())
				                  .doOnTerminate(otherSignalHit::incrementAndGet)
				                  .doOnRequest(r -> requestHit.incrementAndGet())
				                  .doOnCancel(cancelHit::incrementAndGet);
		requestHit.set(0); cancelHit.set(0); nextHit.set(0); errorHit.set(0); otherSignalHit.set(0);
		combiningWithNonNulls.as(StepVerifier::create)
		                     .expectNoFusionSupport()
		                     .expectNext(1)
		                     .verifyComplete();

		assertThat(otherSignalHit).as("sub/terminate x 2").hasValue(4);
		assertThat(requestHit).as("request hit twice").hasValue(2);
		assertThat(cancelHit).as("cancel never hit").hasValue(0);
		assertThat(nextHit).as("onNext hit twice").hasValue(2);
		assertThat(errorHit).as("error never hit").hasValue(0);

		assertThat(Scannable.from(combiningWithNonNulls).steps())
				.as("only one peek publisher")
				.containsExactly("source(MonoJust)", "hide", "peek");

		assertThat(Scannable.from(subRef.get()).steps())
				.as("only one peek subscriber")
				.containsOnlyOnce("peek");
	}

	@Test
	public void macroFusionNormal_error() {
		AtomicInteger errorHit = new AtomicInteger();
		AtomicInteger terminateHit = new AtomicInteger();

		final Mono<Integer> combiningWithNulls = Mono.just(0)
		                                             .map(i -> 100 / i)
		                                             .hide()
		                                             .doOnSubscribe(sub -> {}) //init operator with most handlers null
		                                             .doOnError(e -> errorHit.incrementAndGet())
		                                             .doOnTerminate(terminateHit::incrementAndGet); //afterTerminate is not the same underlying operator

		combiningWithNulls.as(StepVerifier::create)
		                  .expectNoFusionSupport()
		                  .verifyError(ArithmeticException.class);

		assertThat(errorHit).as("error hit").hasValue(1);
		assertThat(terminateHit).as("terminate hit").hasValue(1);

		combiningWithNulls
				.doOnError(e -> errorHit.incrementAndGet())
				.doOnTerminate(terminateHit::incrementAndGet)
				.as(StepVerifier::create)
				.expectNoFusionSupport()
				.verifyError(ArithmeticException.class);

		//3 because of the 1 hit before, plus the second round combining each counter hit twice
		assertThat(errorHit).as("error hit twice").hasValue(3);
		assertThat(terminateHit).as("terminate hit twice").hasValue(3);
	}

	@Test
	public void macroFusionNormal_cancel() {
		AtomicInteger cancelHit = new AtomicInteger();

		final Mono<Integer> combiningWithNulls = Mono
				.just(1)
				.hide()
				.doOnSubscribe(sub -> {}) //init operator with most handlers null
				.doOnCancel(cancelHit::incrementAndGet);

		StepVerifier.create(combiningWithNulls)
		            .expectNoFusionSupport()
		            .thenCancel()
		            .verify();

		assertThat(cancelHit).as("cancel hit").hasValue(1);

		StepVerifier.create(combiningWithNulls.doOnCancel(cancelHit::incrementAndGet))
		            .expectNoFusionSupport()
		            .thenCancel()
		            .verify();

		//3 because of the 1 hit before, plus the second round combining cancel counter hit twice
		assertThat(cancelHit).as("cancel hit twice").hasValue(3);
	}

	@Test
	public void macroFusionWithFuseable() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();
		AtomicInteger requestHit = new AtomicInteger();
		AtomicInteger cancelHit = new AtomicInteger();
		AtomicInteger nextHit = new AtomicInteger();
		AtomicInteger errorHit = new AtomicInteger();
		AtomicInteger otherSignalHit = new AtomicInteger();


		final Mono<Integer> combiningWithNulls = Mono
				.just(1)
				.doOnSubscribe(subRef::set) //basis for first operator, all other handlers are null so far
				.doOnSubscribe(sub -> otherSignalHit.incrementAndGet()) //combine the subscribe handlers
				.doOnNext(v -> nextHit.incrementAndGet())
				.doOnError(e -> errorHit.incrementAndGet())
				.doOnTerminate(otherSignalHit::incrementAndGet)
				.doOnRequest(r -> requestHit.incrementAndGet())
				.doOnCancel(cancelHit::incrementAndGet);

		combiningWithNulls.as(StepVerifier::create)
		                  .expectFusion()
		                  .expectNext(1)
		                  .verifyComplete();

		assertThat(otherSignalHit).as("sub/terminate").hasValue(2);
		assertThat(requestHit).as("request never hit due to fusion").hasValue(0);
		assertThat(cancelHit).as("cancel never hit").hasValue(0);
		assertThat(nextHit).as("onNext hit").hasValue(1);
		assertThat(errorHit).as("error never hit").hasValue(0);

		final Mono<Integer> combiningWithNonNulls =
				combiningWithNulls.doOnSubscribe(subRef::set) //basis for first operator, all other handlers are null so far
				                  .doOnSubscribe(sub -> otherSignalHit.incrementAndGet()) //combine the subscribe handlers
				                  .doOnNext(v -> nextHit.incrementAndGet())
				                  .doOnError(e -> errorHit.incrementAndGet())
				                  .doOnTerminate(otherSignalHit::incrementAndGet)
				                  .doOnRequest(r -> requestHit.incrementAndGet())
				                  .doOnCancel(cancelHit::incrementAndGet);
		requestHit.set(0); cancelHit.set(0); nextHit.set(0); errorHit.set(0); otherSignalHit.set(0);
		combiningWithNonNulls.as(StepVerifier::create)
		                     .expectFusion()
		                     .expectNext(1)
		                     .verifyComplete();

		assertThat(otherSignalHit).as("sub/terminate x 2").hasValue(4);
		assertThat(requestHit).as("request still never hit due to fusion").hasValue(0);
		assertThat(cancelHit).as("cancel still never hit").hasValue(0);
		assertThat(nextHit).as("onNext hit twice").hasValue(2);
		assertThat(errorHit).as("error never hit").hasValue(0);

		assertThat(Scannable.from(combiningWithNonNulls).steps())
				.as("only one peek publisher")
				.containsExactly("source(MonoJust)", "peek");

		assertThat(Scannable.from(subRef.get()).steps())
				.as("only one peek subscriber")
				.containsOnlyOnce("peek");
	}

	@Test
	public void macroFusionWithFuseable_error() {
		AtomicInteger errorHit = new AtomicInteger();
		AtomicInteger terminateHit = new AtomicInteger();

		final Mono<Integer> combiningWithNulls = Mono.just(0)
		                                             .map(i -> 100 / i)
		                                             .log()
		                                             .doOnSubscribe(sub -> {}) //init operator with most handlers null
		                                             .doOnError(e -> errorHit.incrementAndGet())
		                                             .doOnTerminate(terminateHit::incrementAndGet);

		combiningWithNulls.as(StepVerifier::create)
		                  .expectFusion()
		                  .verifyError(ArithmeticException.class);

		assertThat(errorHit).as("error hit").hasValue(1);
		assertThat(terminateHit).as("terminate hit").hasValue(1);

		combiningWithNulls
				.doOnError(e -> errorHit.incrementAndGet())
				.doOnTerminate(terminateHit::incrementAndGet)
				.as(StepVerifier::create)
				.expectFusion()
				.verifyError(ArithmeticException.class);

		//3 because of the 1 hit before, plus the second round combining each counter hit twice
		assertThat(errorHit).as("error hit twice").hasValue(3);
		assertThat(terminateHit).as("terminate hit twice").hasValue(3);
	}

	@Test
	public void macroFusionWithFuseable_cancel() {
		AtomicInteger cancelHit = new AtomicInteger();

		final Mono<Integer> combiningWithNulls = Mono.just(1)
		                                             .doOnSubscribe(Subscription::cancel)
		                                             .doOnCancel(cancelHit::incrementAndGet);

		StepVerifier.create(combiningWithNulls)
		            .expectFusion()
		            .thenCancel()
		            .verify();

		assertThat(cancelHit).as("cancel hit").hasValue(1);

		StepVerifier.create(combiningWithNulls.doOnCancel(cancelHit::incrementAndGet))
		            .expectFusion()
		            .thenCancel()
		            .verify();

		//3 because of the 1 hit before, plus the second round combining cancel counter hit twice
		assertThat(cancelHit).as("cancel hit twice").hasValue(3);
	}
}