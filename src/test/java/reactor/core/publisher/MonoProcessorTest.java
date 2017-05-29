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
package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoProcessorTest {

	@Test(expected = IllegalStateException.class)
	public void MonoProcessorResultNotAvailable() {
		MonoProcessor<String> mp = MonoProcessor.create();
		mp.block(Duration.ofMillis(1));
	}

	@Test
	public void MonoProcessorRejectedDoOnTerminate() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnTerminate((s, f) -> ref.set(f)).subscribe();
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void MonoProcessorRejectedSubscribeCallback() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.subscribe(v -> {}, ref::set);
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void MonoProcessorSuccessDoOnTerminate() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnTerminate((s, f) -> ref.set(s)).subscribe();
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorSuccessSubscribeCallback() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<String> ref = new AtomicReference<>();

		mp.subscribe(ref::set);
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorRejectedDoOnError() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set).subscribe();
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test(expected = NullPointerException.class)
	public void MonoProcessorRejectedSubscribeCallbackNull() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.subscribe((Subscriber<String>)null);
	}

	@Test
	public void MonoProcessorSuccessDoOnSuccess() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnSuccess(ref::set).subscribe();
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorSuccessChainTogether() {
		MonoProcessor<String> mp = MonoProcessor.create();
		MonoProcessor<String> mp2 = MonoProcessor.create();
		mp.subscribe(mp2);

		mp.onNext("test");

		assertThat(mp2.peek()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorRejectedChainTogether() {
		MonoProcessor<String> mp = MonoProcessor.create();
		MonoProcessor<String> mp2 = MonoProcessor.create();
		mp.subscribe(mp2);

		mp.onError(new Exception("test"));

		assertThat(mp2.getError()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test(expected = RuntimeException.class)
	public void MonoProcessorDoubleFulfill() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.onNext("test");
		mp.onNext("test");
	}

	@Test
	public void MonoProcessorNullFulfill() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.onNext(null);

		assertThat(mp.isTerminated()).isTrue();
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.peek()).isNull();
	}

	@Test
	public void MonoProcessorMapFulfill() {
		MonoProcessor<Integer> mp = MonoProcessor.create();

		mp.onNext(1);

		MonoProcessor<Integer> mp2 = mp.map(s -> s * 2)
		                               .toProcessor();
		mp2.subscribe();

		assertThat(mp2.isTerminated()).isTrue();
		assertThat(mp2.isSuccess()).isTrue();
		assertThat(mp2.peek()).isEqualTo(2);
	}

	@Test
	public void MonoProcessoThenFulfill() {
		MonoProcessor<Integer> mp = MonoProcessor.create();

		mp.onNext(1);

		MonoProcessor<Integer> mp2 = mp.flatMap(s -> Mono.just(s * 2))
		                               .toProcessor();
		mp2.subscribe();

		assertThat(mp2.isTerminated()).isTrue();
		assertThat(mp2.isSuccess()).isTrue();
		assertThat(mp2.peek()).isEqualTo(2);
	}

	@Test
	public void MonoProcessorMapError() {
		MonoProcessor<Integer> mp = MonoProcessor.create();

		mp.onNext(1);

		MonoProcessor<Integer> mp2 = MonoProcessor.create();

		StepVerifier.create(mp.<Integer>map(s -> {
			throw new RuntimeException("test");
		}).subscribeWith(mp2), 0)
		            .thenRequest(1)
		            .then(() -> {
			            assertThat(mp2.isTerminated()).isTrue();
			            assertThat(mp2.isSuccess()).isFalse();
			            assertThat(mp2.getError()).hasMessage("test");
		            })
		            .verifyErrorMessage("test");
	}

	@Test(expected = Exception.class)
	public void MonoProcessorDoubleError() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.onError(new Exception("test"));
		mp.onError(new Exception("test"));
	}

	@Test(expected = Exception.class)
	public void MonoProcessorDoubleSignal() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.onNext("test");
		mp.onError(new Exception("test"));
	}

	@Test
	public void whenMonoProcessor() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
		MonoProcessor<Tuple2<Integer, Integer>> mp3 = MonoProcessor.create();

		StepVerifier.create(Mono.when(mp, mp2)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isPending()).isTrue())
		            .then(() -> mp.onNext(1))
		            .then(() -> assertThat(mp3.isPending()).isTrue())
		            .then(() -> mp2.onNext(2))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isTrue();
			            assertThat(mp3.isPending()).isFalse();
			            assertThat(mp3.peek()
			                          .getT1()).isEqualTo(1);
			            assertThat(mp3.peek()
			                          .getT2()).isEqualTo(2);
		            })
		            .expectNextMatches(t -> t.getT1() == 1 && t.getT2() == 2)
		            .verifyComplete();
	}

	@Test
	public void whenMonoProcessor2() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp3 = MonoProcessor.create();

		StepVerifier.create(Mono.when(d -> (Integer)d[0], mp)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isPending()).isTrue())
		            .then(() -> mp.onNext(1))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isTrue();
			            assertThat(mp3.isPending()).isFalse();
			            assertThat(mp3.peek()).isEqualTo(1);
		            })
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void whenMonoProcessorRejected() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
		MonoProcessor<Tuple2<Integer, Integer>> mp3 = MonoProcessor.create();

		StepVerifier.create(Mono.when(mp, mp2)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isPending()).isTrue())
		            .then(() -> mp.onError(new Exception("test")))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isFalse();
			            assertThat(mp3.isPending()).isFalse();
			            assertThat(mp3.getError()).hasMessage("test");
		            })
		            .verifyErrorMessage("test");
	}


	@Test
	public void filterMonoProcessor() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
		StepVerifier.create(mp.filter(s -> s % 2 == 0).subscribeWith(mp2))
		            .then(() -> mp.onNext(2))
		            .then(() -> assertThat(mp2.isError()).isFalse())
		            .then(() -> assertThat(mp2.isSuccess()).isTrue())
		            .then(() -> assertThat(mp2.peek()).isEqualTo(2))
		            .then(() -> assertThat(mp2.isTerminated()).isTrue())
		            .expectNext(2)
		            .verifyComplete();
	}


	@Test
	public void filterMonoProcessorNot() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
		StepVerifier.create(mp.filter(s -> s % 2 == 0).subscribeWith(mp2))
		            .then(() -> mp.onNext(1))
		            .then(() -> assertThat(mp2.isError()).isFalse())
		            .then(() -> assertThat(mp2.isSuccess()).isTrue())
		            .then(() -> assertThat(mp2.peek()).isNull())
		            .then(() -> assertThat(mp2.isTerminated()).isTrue())
		            .verifyComplete();
	}

	@Test
	public void filterMonoProcessorError() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
		StepVerifier.create(mp.filter(s -> {throw new RuntimeException("test"); })
					.subscribeWith
						(mp2))
		            .then(() -> mp.onNext(2))
		            .then(() -> assertThat(mp2.isError()).isTrue())
		            .then(() -> assertThat(mp2.isSuccess()).isFalse())
		            .then(() -> assertThat(mp2.getError()).hasMessage("test"))
		            .then(() -> assertThat(mp2.isTerminated()).isTrue())
		            .verifyErrorMessage("test");
	}

	@Test
	public void doOnSuccessMonoProcessorError() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		StepVerifier.create(mp.doOnSuccess(s -> {throw new RuntimeException("test"); })
		                      .doOnError(ref::set)
					.subscribeWith
						(mp2))
		            .then(() -> mp.onNext(2))
		            .then(() -> assertThat(mp2.isError()).isTrue())
		            .then(() -> assertThat(ref.get()).hasMessage("test"))
		            .then(() -> assertThat(mp2.isSuccess()).isFalse())
		            .then(() -> assertThat(mp2.getError()).hasMessage("test"))
		            .then(() -> assertThat(mp2.isTerminated()).isTrue())
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCancelledByMonoProcessor() {
		AtomicLong cancelCounter = new AtomicLong();
		Flux.range(1, 10)
		    .doOnCancel(cancelCounter::incrementAndGet)
		    .publishNext()
		    .subscribe();

		assertThat(cancelCounter.get()).isEqualTo(1);
	}

	@Test
	public void monoNotCancelledByMonoProcessor() {
		AtomicLong cancelCounter = new AtomicLong();
		MonoProcessor<String> monoProcessor = Mono.just("foo")
		                                          .doOnCancel(cancelCounter::incrementAndGet)
		                                          .toProcessor();
		monoProcessor.subscribe();

		assertThat(cancelCounter.get()).isEqualTo(0);
	}

	@Test
	public void scanProcessor() {
		MonoProcessor<String> test = MonoProcessor.create();
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		test.onComplete();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
	}

	@Test
	public void scanProcessorCancelled() {
		MonoProcessor<String> test = MonoProcessor.create();
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanProcessorSubscription() {
		MonoProcessor<String> test = MonoProcessor.create();
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(subscription);

		test.getOrStart();
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isNotNull();
	}

	@Test
	public void scanProcessorError() {
		MonoProcessor<String> test = MonoProcessor.create();
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		test.onError(new IllegalStateException("boom"));

		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

}