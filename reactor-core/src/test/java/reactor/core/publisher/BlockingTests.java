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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.AssertionsForClassTypes.*;

public class BlockingTests {

	static Scheduler scheduler;
	static Scheduler nonBlockingScheduler;

	@BeforeClass
	public static void before() {
		scheduler = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor());
		nonBlockingScheduler = Schedulers.newSingle("nonBlockingScheduler");
	}

	@AfterClass
	public static void after() {
		scheduler.dispose();
		nonBlockingScheduler.dispose();
	}

	@Test
	public void blockingFirst() {
		Assert.assertEquals((Integer) 1,
				Flux.range(1, 10)
				    .publishOn(scheduler)
				    .blockFirst());
	}

	@Test
	public void blockingFirst2() {
		Assert.assertEquals((Integer) 1,
				Flux.range(1, 10)
				    .publishOn(scheduler)
				    .blockFirst(Duration.ofSeconds(10)));
	}

	@Test
	public void blockingFirstTimeout() {
		assertThat(Flux.empty()
		               .blockFirst(Duration.ofMillis(1))).isNull();
	}

	@Test
	public void blockingLast() {
		Assert.assertEquals((Integer) 10,
				Flux.range(1, 10)
				    .publishOn(scheduler)
				    .blockLast());
	}

	@Test
	public void blockingLast2() {
		Assert.assertEquals((Integer) 10,
				Flux.range(1, 10)
				    .publishOn(scheduler)
				    .blockLast(Duration.ofSeconds(10)));
	}

	@Test
	public void blockingLastTimeout() {
		assertThat(Flux.empty()
		               .blockLast(Duration.ofMillis(1))).isNull();
	}

	@Test(expected = RuntimeException.class)
	public void blockingFirstError() {
		Flux.error(new RuntimeException("test"))
		    .publishOn(scheduler)
		    .blockFirst();
	}

	@Test(expected = RuntimeException.class)
	public void blockingFirstError2() {
		Flux.error(new RuntimeException("test"))
		    .publishOn(scheduler)
		    .blockFirst(Duration.ofSeconds(1));
	}

	@Test(expected = RuntimeException.class)
	public void blockingLastError() {
		Flux.defer(() -> Mono.error(new RuntimeException("test")))
		    .subscribeOn(scheduler)
		    .blockLast();
	}

	@Test(expected = RuntimeException.class)
	public void blockingLastError2() {
		Flux.defer(() -> Mono.error(new RuntimeException("test")))
		    .subscribeOn(scheduler)
		    .blockLast(Duration.ofSeconds(1));
	}

	@Test
	public void blockingLastInterrupted() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		Thread t = new Thread(() -> {
			try {
				Flux.never()
				    .blockLast();
			}
			catch (Exception e) {
				if (Exceptions.unwrap(e) instanceof InterruptedException) {
					latch.countDown();
				}
			}
		});

		t.start();
		Thread.sleep(1000);
		t.interrupt();

		Assert.assertTrue("Not interrupted ?", latch.await(3, TimeUnit.SECONDS));
	}

	/*@Test
	public void fillIn() throws Exception {
		Path sourcePath = Paths.get(
				"/Users/smaldini/work/reactor-core/src/main/java/reactor/core/publisher");

		String template =
				"package reactor.core.publisher;\n\nimport org.junit.Test;\n\npublic " + "class {name} { @Test public" + " void normal(){} }";

		Flux.fromStream(Files.list(sourcePath))
		    .map(Path::toFile)
		    .filter(f -> f.getName()
		                  .startsWith("Flux") || f.getName()
		                                          .startsWith("Mono"))
		    .map(f -> {
			    try {
				    return new File(f.getAbsolutePath()
				                     .replace("main", "test")
				                     .replace(".java", "Test.java"));
			    }
			    catch (Exception t) {
				    throw Exceptions.propagate(t);
			    }
		    })
		    .filter(f -> {
			    try {
				    return f.createNewFile();
			    }
			    catch (Exception t) {
				    throw Exceptions.propagate(t);
			    }
		    })
		    .doOnNext(f -> {
			    try (FileOutputStream fo = new FileOutputStream(f)) {
				    fo.write(template.replace("{name}",
						    f.getName()
						     .replace(".java", ""))
				                     .getBytes());
			    }
			    catch (Exception t) {
				    throw Exceptions.propagate(t);
			    }
		    })
		    .subscribe(System.out::println);
	}*/

	@Test
	public void fluxBlockFirstCancelsOnce() {
		AtomicLong cancelCount = new AtomicLong();
		Flux.range(1, 10)
	        .doOnCancel(cancelCount::incrementAndGet)
	        .blockFirst();

		assertThat(cancelCount.get()).isEqualTo(1);
	}

	@Test
	public void fluxBlockLastDoesntCancel() {
		AtomicLong cancelCount = new AtomicLong();
		Flux.range(1, 10)
	        .doOnCancel(cancelCount::incrementAndGet)
	        .blockLast();

		assertThat(cancelCount.get()).isEqualTo(0);
	}

	@Test
	public void monoBlockDoesntCancel() {
		AtomicLong cancelCount = new AtomicLong();
		Mono.just("data")
	        .doOnCancel(cancelCount::incrementAndGet)
	        .block();

		assertThat(cancelCount.get()).isEqualTo(0);
	}

	@Test
	public void monoBlockOptionalDoesntCancel() {
		AtomicLong cancelCount = new AtomicLong();
		Mono.just("data")
	        .doOnCancel(cancelCount::incrementAndGet)
	        .blockOptional();

		assertThat(cancelCount.get()).isEqualTo(0);
	}

	@Test
	public void fluxBlockFirstForbidden() {
		Function<String, String> badMapper = v -> Flux.just(v).hide()
		                                              .blockFirst();
		Function<String, String> badMapperTimeout = v -> Flux.just(v).hide()
		                                                     .blockFirst(Duration.ofMillis(100));

		Mono<String> forbiddenSequence1 = Mono.just("data")
		                                     .publishOn(nonBlockingScheduler)
		                                     .map(badMapper);

		StepVerifier.create(forbiddenSequence1)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessageStartingWith("block()/blockFirst()/blockLast() are blocking, which is not supported in thread nonBlockingScheduler-"))
		            .verify();

		Mono<String> forbiddenSequence2 = Mono.just("data")
		                                     .publishOn(nonBlockingScheduler)
		                                     .map(badMapperTimeout);

		StepVerifier.create(forbiddenSequence2)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessageStartingWith("block()/blockFirst()/blockLast() are blocking, which is not supported in thread nonBlockingScheduler-"))
		            .verify();
	}

	@Test
	public void fluxBlockLastForbidden() {
		Function<String, String> badMapper = v -> Flux.just(v).hide()
		                                              .blockLast();
		Function<String, String> badMapperTimeout = v -> Flux.just(v).hide()
		                                                     .blockLast(Duration.ofMillis(100));

		Mono<String> forbiddenSequence1 = Mono.just("data")
		                                     .publishOn(nonBlockingScheduler)
		                                     .map(badMapper);

		StepVerifier.create(forbiddenSequence1)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessageStartingWith("block()/blockFirst()/blockLast() are blocking, which is not supported in thread nonBlockingScheduler-"))
		            .verify();

		Mono<String> forbiddenSequence2 = Mono.just("data")
		                                     .publishOn(nonBlockingScheduler)
		                                     .map(badMapperTimeout);

		StepVerifier.create(forbiddenSequence2)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessageStartingWith("block()/blockFirst()/blockLast() are blocking, which is not supported in thread nonBlockingScheduler-"))
		            .verify();
	}

	@Test
	public void monoBlockForbidden() {
		Function<String, String> badMapper = v -> Mono.just(v).hide()
		                                              .block();
		Function<String, String> badMapperTimeout = v -> Mono.just(v).hide()
		                                                     .block(Duration.ofMillis(100));

		Mono<String> forbiddenSequence1 = Mono.just("data")
		                                     .publishOn(nonBlockingScheduler)
		                                     .map(badMapper);

		StepVerifier.create(forbiddenSequence1)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessageStartingWith("block()/blockFirst()/blockLast() are blocking, which is not supported in thread nonBlockingScheduler-"))
		            .verify();

		Mono<String> forbiddenSequence2 = Mono.just("data")
		                                     .publishOn(nonBlockingScheduler)
		                                     .map(badMapperTimeout);

		StepVerifier.create(forbiddenSequence2)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessageStartingWith("block()/blockFirst()/blockLast() are blocking, which is not supported in thread nonBlockingScheduler-"))
		            .verify();
	}

	@Test
	public void monoBlockOptionalForbidden() {
		Function<String, Optional<String>> badMapper = v -> Mono.just(v).hide()
		                                                        .blockOptional();
		Function<String, Optional<String>> badMapperTimeout = v -> Mono.just(v).hide()
		                                                               .blockOptional(Duration.ofMillis(100));

		Mono<Optional<String>> forbiddenSequence1 = Mono.just("data")
		                                                .publishOn(nonBlockingScheduler)
		                                                .map(badMapper);

		StepVerifier.create(forbiddenSequence1)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessageStartingWith("blockOptional() is blocking, which is not supported in thread nonBlockingScheduler-"))
		            .verify();

		Mono<Optional<String>> forbiddenSequence2 = Mono.just("data")
		                                                .publishOn(nonBlockingScheduler)
		                                                .map(badMapperTimeout);

		StepVerifier.create(forbiddenSequence2)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessageStartingWith("blockOptional() is blocking, which is not supported in thread nonBlockingScheduler-"))
		            .verify();
	}

	@Test
	public void fluxToIterableOkButIterationForbidden() throws InterruptedException {
		AtomicReference<Iterable<Integer>> ref = new AtomicReference<>();
		AtomicReference<Throwable> refError = new AtomicReference<>();
		final CountDownLatch latch1 = new CountDownLatch(1);

		nonBlockingScheduler.schedule(() -> {
			try {
				ref.set(Flux.just(1, 2, 3)
				            .toIterable());
			}
			catch (Throwable e) {
				refError.set(e);
			}
			finally {
				latch1.countDown();
			}
		});

		latch1.await(1, TimeUnit.SECONDS);

		assertThat(ref.get()).as("iterable").isNotNull();
		assertThat(refError.get()).as("error").isNull();

		AtomicReference<Throwable> assertionRef = new AtomicReference<>();
		final CountDownLatch latch2 = new CountDownLatch(1);

		//we want to check how some operations on the iterable behave WITHIN A NONBLOCKING Thread
		nonBlockingScheduler.schedule(() -> {
			Iterable<Integer> iterable = ref.get();
			try {
				assertThatCode(iterable::iterator).as("iterator()").doesNotThrowAnyException();
				assertThatCode(iterable::spliterator).as("spliterator()").doesNotThrowAnyException();

				assertThatExceptionOfType(IllegalStateException.class)
						.isThrownBy(() -> iterable.forEach(v -> {}))
						.as("forEach")
						.withMessageStartingWith("Iterating over a toIterable() / toStream() is blocking, which is not supported in thread nonBlockingScheduler-");
			}
			catch (Throwable e) {
				assertionRef.set(e);
			}
			finally {
				latch2.countDown();
			}
		});

		latch2.await(1, TimeUnit.SECONDS);

		assertThat(assertionRef.get()).as("assertions pass within scheduler").isNull();
	}

	@Test
	public void fluxToStreamOkButIterationForbidden() throws InterruptedException {
		AtomicReference<Stream<Integer>> ref = new AtomicReference<>();
		AtomicReference<Throwable> refError = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);

		nonBlockingScheduler.schedule(() -> {
			try {
				ref.set(Flux.just(1, 2, 3)
				            .toStream());
			}
			catch (Throwable e) {
				refError.set(e);
			}
			finally {
				latch.countDown();
			}
		});

		latch.await(1, TimeUnit.SECONDS);

		assertThat(ref.get()).as("stream").isNotNull();
		assertThat(refError.get()).as("error").isNull();

		AtomicReference<Throwable> assertionRef = new AtomicReference<>();
		final CountDownLatch latch2 = new CountDownLatch(1);

		//we want to check how some operations on the iterable behave WITHIN A NONBLOCKING Thread
		nonBlockingScheduler.schedule(() -> {
			Stream<Integer> stream = ref.get();
			try {
				//stream can only be operator on once, so we need to store the intermediate result in the ref...
				assertThatCode(() -> ref.set(stream.distinct())).as("intermediate operator (distinct)").doesNotThrowAnyException();
				//...and work again from that updated ref
				assertThatExceptionOfType(IllegalStateException.class)
						.isThrownBy(() -> ref.get().count())
						.as("terminal operator (count)")
						.withMessageStartingWith("Iterating over a toIterable() / toStream() is blocking, which is not supported in thread nonBlockingScheduler-");
			}
			catch (Throwable e) {
				assertionRef.set(e);
			}
			finally {
				latch2.countDown();
			}
		});

		latch2.await(1, TimeUnit.SECONDS);

		assertThat(assertionRef.get()).as("assertions pass within scheduler").isNull();
	}
}
