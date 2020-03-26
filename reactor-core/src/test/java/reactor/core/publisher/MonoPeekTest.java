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
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoPeekTest {

	@Test
	public void onMonoRejectedDoOnSuccessOrError() {
		Mono<String> mp = Mono.error(new Exception("test"));
		AtomicReference<Throwable> ref = new AtomicReference<>();

		@SuppressWarnings("deprecation")
		Mono<String> mono = mp.doOnSuccessOrError((s, f) -> ref.set(f));

		mono.subscribe();

		assertThat(ref.get()).hasMessage("test");
	}

	@Test
	public void onMonoSuccessDoOnSuccessOrError() {
		Mono<String> mp = Mono.just("test");
		AtomicReference<String> ref = new AtomicReference<>();

		@SuppressWarnings("deprecation")
		Mono<String> mono = mp.doOnSuccessOrError((s, f) -> ref.set(s));

		mono.subscribe();

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
}