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

package reactor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.ParallelFlux;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class HooksTest {

	void simpleFlux(){
		Flux.just(1)
		    .map(d -> d + 1)
		    .doOnNext(d -> {throw new RuntimeException("test");})
		    .collectList()
		    .onErrorReturn(Collections.singletonList(2))
		    .block();
	}

	static final class TestException extends RuntimeException {

		public TestException(String message) {
			super(message);
		}
	}

	@Test
	public void errorHooks() throws Exception {

		Hooks.onOperatorError((e, s) -> new TestException(s.toString()));
		Hooks.onNextDropped(d -> {
			throw new TestException(d.toString());
		});
		Hooks.onErrorDropped(e -> {
			throw new TestException("errorDrop");
		});

		Throwable w = Operators.onOperatorError(null, new Exception(), "hello");

		Assert.assertTrue(w instanceof TestException);
		Assert.assertTrue(w.getMessage()
		                   .equals("hello"));

		try {
			Operators.onNextDropped("hello");
			Assert.fail();
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.assertTrue(t instanceof TestException);
			Assert.assertTrue(t.getMessage()
			                   .equals("hello"));
		}

		try {
			Operators.onErrorDropped(new Exception());
			Assert.fail();
		}
		catch (Throwable t) {
			Assert.assertTrue(t instanceof TestException);
			Assert.assertTrue(t.getMessage()
			                   .equals("errorDrop"));
		}

		Hooks.resetOnOperatorError();
		Hooks.resetOnNextDropped();
		Hooks.resetOnErrorDropped();
	}

	@Test
	public void parallelModeFused() {
		Hooks.onOperator(h -> h.log("", Level.INFO, true, SignalType.ON_COMPLETE).operatorStacktrace());
		Flux<Integer> source = Mono.just(1)
		                           .flux()
		                           .repeat(1000)
		                           .publish()
		                           .autoConnect();
		int ncpu = Math.max(8,
				Runtime.getRuntime()
				       .availableProcessors());

			Scheduler scheduler = Schedulers.newParallel("test", ncpu);

			try {
				Flux<Integer> result = ParallelFlux.from(source, ncpu)
				                                   .runOn(scheduler)
				                                   .map(v -> v + 1)
				                                   .log("test", Level.INFO, true, SignalType.ON_SUBSCRIBE)
				                                   .sequential();

				AssertSubscriber<Integer> ts = AssertSubscriber.create();

				result.subscribe(ts);

				ts.await(Duration.ofSeconds(10));

				ts.assertSubscribed()
				  .assertValueCount(1000)
				  .assertComplete()
				  .assertNoError();
			}
			finally {
				Hooks.resetOnOperator();
				scheduler.dispose();
			}

	}

	@Test
	public void verboseExtension() {
		Queue<String> q = new LinkedTransferQueue<>();

		Hooks.onOperator(hooks -> hooks.operatorStacktrace()
		                               .doOnEach(d -> q.offer(hooks.publisher() + ": " + d),
				                               t -> q.offer(hooks.publisher() + "! " +
						                               (t.getSuppressed().length != 0)),
				                               null,
				                               null));

		simpleFlux();

		Assert.assertArrayEquals(q.toArray(),
				new String[]{"FluxJust: 1", "{ \"operator\" : \"MapFuseable\" }: 2",
						"{ \"operator\" : \"PeekFuseable\" }! false",
						"{ \"operator\" : \"CollectList\" }! true", "MonoJust: [2]",
						"{ \"operator\" : \"OnErrorResume\" }: [2]"});

		q.clear();

		Hooks.onOperator(hooks -> hooks.log("reactor", true)
		                               .doOnEach(d -> q.offer(hooks.publisher() + ": " + d),
				                               t -> q.offer(hooks.publisher() + "! " +
						                               (t.getSuppressed().length != 0)),
				                               null,
				                               null));

		simpleFlux();

		Assert.assertArrayEquals(q.toArray(),
				new String[]{"FluxJust: 1", "{ \"operator\" : \"MapFuseable\" }: 2",
						"{ \"operator\" : \"PeekFuseable\" }! false",
						"{ \"operator\" : \"CollectList\" }! false", "MonoJust: [2]",
						"{ \"operator\" : \"OnErrorResume\" }: [2]"});

		q.clear();

		Hooks.onOperator(hooks -> hooks.log("reactor"));

		simpleFlux();

		Assert.assertArrayEquals(q.toArray(), new String[0]);

		q.clear();

		Hooks.resetOnOperator();

		simpleFlux();
	}


	@Test
	public void testTrace() throws Exception {
		Hooks.onOperator(Hooks.OperatorHook::operatorStacktrace);
		try {
			Mono.fromCallable(() -> {
				throw new RuntimeException();
			})
			    .map(d -> d)
			    .block();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("MonoCallable"));
			return;
		}
		finally {
			Hooks.resetOnOperator();
		}
		throw new IllegalStateException();
	}


	@Test
	public void testTrace2() throws Exception {
		Hooks.onOperator(hooks -> hooks.ifName("map", "filter")
		                               .operatorStacktrace());
		try {
			Mono.just(1)
			    .map(d -> {
				    throw new RuntimeException();
			    })
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .block();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("HooksTest.java:"));
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("|_\tMono.map" +
					"(HooksTest.java:"));
			return;
		}
		finally {
			Hooks.resetOnOperator();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testTrace3() throws Exception {
		Hooks.onOperator(hooks -> hooks.operatorStacktrace());
		try {
			Flux.just(1)
			    .map(d -> {
				    throw new RuntimeException();
			    })
			    .share()
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .blockLast();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("HooksTest.java:"));
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("|_\tFlux" +
					".publish" +
					"(HooksTest.java:"));
			return;
		}
		finally {
			Hooks.resetOnOperator();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testTraceComposed() throws Exception {
		Hooks.onOperator(hooks -> hooks.operatorStacktrace());
		try {
			Mono.just(1)
			    .flatMap(d ->
				    Mono.error(new RuntimeException())
			    )
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .block();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("HooksTest.java:"));
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("|_\tMono" +
					".flatMap" +
					"(HooksTest.java:"));
			return;
		}
		finally {
			Hooks.resetOnOperator();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testTraceComposed2() throws Exception {
		Hooks.onOperator(hooks -> hooks.operatorStacktrace());
		try {
			Flux.just(1)
			    .flatMap(d -> {
				    throw new RuntimeException();
			    })
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .blockLast();
		}
		catch(Exception e){
			e.printStackTrace();
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("HooksTest.java:"));
			Assert.assertTrue(e.getSuppressed()[0].getMessage().contains("|_\tFlux" +
					".flatMap" +
					"(HooksTest.java:"));
			return;
		}
		finally {
			Hooks.resetOnOperator();
		}
		throw new IllegalStateException();
	}

	@Test
	public void testOnSubscriber() throws Exception {
		List<Subscriber> l = new ArrayList<>();
		Hooks.onSubscriber((s, c) -> {
			l.add(s);
			return s;
		});
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .map(m -> m)
		                        .takeUntilOther(Mono.never())
		                        .flatMap(d -> Mono.just(d).hide()))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		Hooks.resetOnSubscriber();

		assertThat(l).hasSize(5);
	}

	@Test
	public void testMultiReceiver() throws Exception {
		Hooks.onOperator(hooks -> hooks.operatorStacktrace());
		try {

			ConnectableFlux<?> t = Flux.empty()
			    .then(Mono.defer(() -> {
				    throw new RuntimeException();
			    })).flux().publish();

			t.map(d -> d).subscribe(null,
					e -> Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
							("\t|_\tFlux.publish")));

			t.filter(d -> true).subscribe(null, e -> Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("\t\t|_\tFlux.publish")));
			t.distinct().subscribe(null, e -> Assert.assertTrue(e.getSuppressed()[0].getMessage().contains
					("\t\t\t|_\tFlux.publish")));

			t.connect();
		}
		finally {
			Hooks.resetOnOperator();
		}
	}

}
