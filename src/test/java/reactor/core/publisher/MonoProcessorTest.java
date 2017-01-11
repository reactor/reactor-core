/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoProcessorTest {

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
	public void MonoProcessorRejectedDoOnError() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set).subscribe();
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
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
		                               .subscribe();

		assertThat(mp2.isTerminated()).isTrue();
		assertThat(mp2.isSuccess()).isTrue();
		assertThat(mp2.peek()).isEqualTo(2);
	}

	@Test
	public void MonoProcessoThenFulfill() {
		MonoProcessor<Integer> mp = MonoProcessor.create();

		mp.onNext(1);

		MonoProcessor<Integer> mp2 = mp.then(s -> Mono.just(s * 2))
		                               .subscribe();

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

//
//	def "Multiple promises can be combined"() {
//		given: "two fulfilled promises"
//		def bc1 = EmitterProcessor.<Integer> create().connect()
//		def promise1 = bc1.doOnNext { println 'hey' + it }.next()
//		def bc2 = MonoProcessor.<Integer> create()
//		def promise2 = bc2.flux().log().next()
//
//		when: "a combined promise is first created"
//		def combined = Mono.when(promise1, promise2).subscribe()
//
//		then: "it is pending"
//		!combined.pending
//
//		when: "the first promise is fulfilled"
//		bc1.onNext 1
//
//		then: "the combined promise is still pending"
//		!combined.pending
//
//		when: "the second promise if fulfilled"
//		bc2.onNext 2
//
//		then: "the combined promise is fulfilled with both values"
//		combined.block().t1 == 1
//		combined.block().t2 == 2
//		combined.success
//	}
//
//	def "A combined promise is rejected once any of its component promises are rejected"() {
//		given: "two unfulfilled promises"
//		def promise1 = MonoProcessor.<Integer> create()
//		def promise2 = MonoProcessor.<Integer> create()
//
//		when: "a combined promise is first created"
//		def combined = Mono.when(promise1, promise2).subscribeWith(MonoProcessor.create())
//
//		then: "it is pending"
//		!combined.pending
//
//		when: "a component promise is rejected"
//		promise1.onError new Exception()
//
//		then: "the combined promise is rejected"
//		combined.error
//	}
//
//	def "A single promise can be 'combined'"() {
//		given: "one unfulfilled promise"
//		MonoProcessor<Integer> promise1 = MonoProcessor.create()
//
//		when: "a combined promise is first created"
//		def combined = MonoProcessor.create()
//		Mono.when({d -> d } as Function, promise1).log().subscribe(combined)
//
//		then: "it is pending"
//		!combined.pending
//
//		when: "the first promise is fulfilled"
//		promise1.onNext 1
//
//		then: "the combined promise is fulfilled"
//		combined.block(Duration.ofSeconds(1)) == [1]
//		combined.success
//	}
//	def "A filtered promise is not fulfilled if the filter does not allow the value to pass through"() {
//		given: "a promise with a filter that only accepts even values"
//		def promise = MonoProcessor.create()
//		def filtered = promise.flux().filter { it % 2 == 0 }.next()
//
//		when: "the promise is fulfilled with an odd value"
//		promise.onNext 1
//
//		then: "the filtered promise is not fulfilled"
//		!filtered.block()
//	}
//
//	def "A filtered promise is fulfilled if the filter allows the value to pass through"() {
//		given: "a promise with a filter that only accepts even values"
//		def promise = MonoProcessor.create()
//		promise.flux().filter { it % 2 == 0 }.next()
//
//		when: "the promise is fulfilled with an even value"
//		promise.onNext 2
//
//		then: "the filtered promise is fulfilled"
//		promise.success
//		promise.peek() == 2
//	}
//
//	def "If a filter throws an exception the filtered promise is rejected"() {
//		given: "a promise with a filter that throws an error"
//		def promise = MonoProcessor.create()
//		def e = new RuntimeException()
//		def filteredMonoProcessor = promise.flux().filter { throw e }.next()
//
//		when: "the promise is fulfilled"
//		promise.onNext 2
//		filteredMonoProcessor.block()
//
//		then: "the filtered promise is rejected"
//		thrown RuntimeException
//	}
//	def "Errors stop compositions"() {
//		given: "a promise"
//		def p1 = MonoProcessor.<String> create()
//
//		final latch = new CountDownLatch(1)
//
//		when: "p1 is consumed by p2"
//		MonoProcessor p2 = p1.log().doOnSuccess({ Integer.parseInt it }).
//				doOnError{ latch.countDown() }.
//		log().
//				map { println('not in log'); true }.subscribe()
//
//		and: "setting a value"
//		p1.onNext 'not a number'
//		p2.blockMillis(1_000)
//
//		then: 'No value'
//		thrown(RuntimeException)
//		latch.count == 0
//	}
//
//	def "Can poll instead of await to automatically handle InterruptedException"() {
//		given: "a promise"
//		def p1 = MonoProcessor.<String> create()
//		def s = Schedulers.newSingle('test')
//		when: "p1 is consumed by p2"
//		def p2 = p1
//				.publishOn(s)
//				.map {
//			println Thread.currentThread()
//			sleep(3000)
//			Integer.parseInt it
//		}
//
//		and: "setting a value"
//		p1.onNext '1'
//		println "emitted"
//		p2.block(Duration.ofSeconds(1))
//
//		then: 'No value'
//		thrown IllegalStateException
//
//		when: 'polling undefinitely'
//		def v = p2.block()
//
//		then: 'Value!'
//		v
//
//		cleanup:
//		s.dispose()
//	}
}