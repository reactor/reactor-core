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

import org.junit.jupiter.api.Test;
import org.assertj.core.api.Assertions;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.util.LoggerUtils;
import reactor.test.StepVerifier;
import reactor.test.util.TestLogger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoPeekTest {

	@Test
	public void onMonoRejectedDoOnSuccessOrError() {
		Mono<String> mp = Mono.error(new Exception("test"));
		AtomicReference<Throwable> ref = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<String> mono = mp.doOnSuccessOrError((s, f) -> ref.set(f));

		mono.subscribe();

		assertThat(ref.get()).hasMessage("test");
	}

	@Test
	public void onMonoSuccessDoOnSuccessOrError() {
		Mono<String> mp = Mono.just("test");
		AtomicReference<String> ref = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
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

		assertThat(invoked).hasValue(1);
	}

	@Test
	public void onMonoSuccessDoOnTerminate() {
		Mono<String> mp = Mono.just("test");
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet)
		  .subscribe();

		assertThat(invoked).hasValue(1);
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

		assertThat(ref).hasValue(123L);
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

		StepVerifier.create(mp.doOnError(RuntimeException.class, ref::set))
					.verifyError(TestException.class);

		assertThat(ref.get()).isNull();
	}

	@Test
	public void onMonoSuccessNullDoOnSuccess() {
		Mono<String> mp = Mono.just("test");
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			mp.doOnSuccess(null)
					.subscribe();
		});
	}

	@Test
	public void testErrorWithDoOnSuccess() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			Mono.error(new NullPointerException("boom"))
			    .doOnSuccess(aValue -> {
			    })
			    .subscribe();

			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains("reactor.core.Exceptions$ErrorCallbackNotImplemented: java.lang.NullPointerException: boom");
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void scanOperator(){
	    MonoPeek<Integer> test = new MonoPeek<>(Mono.just(1), null, null, null, null);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		MonoPeekFuseable<Integer> test = new MonoPeekFuseable<>(Mono.just(1), null, null, null, null);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
