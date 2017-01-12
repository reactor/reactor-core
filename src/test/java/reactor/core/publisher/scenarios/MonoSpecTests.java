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
package reactor.core.publisher.scenarios;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class MonoSpecTests {

	@Test
	public void onMonoRejectedDoOnTerminate() {
		Mono<String> mp = Mono.error(new Exception("test"));
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnTerminate((s, f) -> ref.set(f))
		  .subscribe();

		assertThat(ref.get()).hasMessage("test");
	}

	@Test
	public void onMonoSuccessDoOnTerminate() {
		Mono<String> mp = Mono.just("test");
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnTerminate((s, f) -> ref.set(s))
		  .subscribe();

		assertThat(ref.get()).isEqualToIgnoringCase("test");
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
	public void onMonoRejectedDoOnError() {
		Mono<String> mp = Mono.error(new Exception("test"));
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set)
		  .subscribe();

		assertThat(ref.get()).hasMessage("test");
	}

	@Test(expected = NullPointerException.class)
	public void onMonoSuccessNullDoOnSuccess() {
		Mono<String> mp = Mono.just("test");
		mp.doOnSuccess(null)
		  .subscribe();
	}

	@Test(expected = Exception.class)
	public void onMonoRejectedThrowOnBlock() {
		Mono.error(new Exception("test"))
		    .block();
	}

	@Test
	public void onMonoSuccessReturnOnBlock() {
		assertThat(Mono.just("test")
		               .block()).isEqualToIgnoringCase("test");
	}

	@Test
	public void onMonoSuccessCallableOnBlock() {
		assertThat(Mono.fromCallable(() -> "test")
		               .block()).isEqualToIgnoringCase("test");
	}

	@Test(expected = RuntimeException.class)
	public void onMonoErrorCallableOnBlock() {
		Mono.fromCallable(() -> {
			throw new Exception("test");
		})
		    .block();
	}

	@Test
	public void mapMono() {
		StepVerifier.create(Mono.just(1)
		                        .map(s -> s * 2))
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void mapMonoRejected() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.just(1)
		                        .<Integer>map(i -> { throw new RuntimeException("test"); })
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isTrue())
		            .then(() -> assertThat(mp.isSuccess()).isFalse())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .verifyErrorMessage("test");
	}

	@Test
	public void firstMonoJust() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.first(Mono.just(1), Mono.just(2))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .expectNext(1)
		            .verifyComplete();
	}

	Mono<Integer> scenario_fastestSource() {
		return Mono.first(Mono.delay(Duration.ofSeconds(4))
		                      .map(s -> 1),
				Mono.delay(Duration.ofSeconds(3))
				    .map(s -> 2));
	}

	@Test
	public void fastestSource() {
		StepVerifier.withVirtualTime(this::scenario_fastestSource)
		            .thenAwait(Duration.ofSeconds(4))
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void filterMono() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.just(2).filter(s -> s % 2 == 0).subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.peek()).isEqualTo(2))
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .expectNext(2)
		            .verifyComplete();
	}


	@Test
	public void filterMonoNot() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.just(1).filter(s -> s % 2 == 0).subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.peek()).isNull())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .verifyComplete();
	}

}
