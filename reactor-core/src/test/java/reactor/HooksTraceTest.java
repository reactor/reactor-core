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

package reactor;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;

import reactor.core.Scannable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Stephane Maldini
 */
public class HooksTraceTest {

	@After
	public void resetHooks() {
		Hooks.resetOnEachOperator();
		Hooks.resetOnLastOperator();
		Hooks.resetOnOperatorDebug();
	}

	@Test
	public void testTrace() {
		Hooks.onOperatorDebug();

		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				Mono.fromCallable(() -> {
					throw new RuntimeException("test");
				})
				    .map(d -> d)
				    .block()
		).satisfies(r -> assertThat(r.getSuppressed()[0]).hasMessageContaining("Assembly trace from producer [reactor.core.publisher.MonoCallable]"));
	}

	@Test
	public void testTrace2() {
		Hooks.onOperatorDebug();

		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
			Mono.just(1)
			    .map(d -> {
				    throw new RuntimeException();
			    })
			    .filter(d -> true)
			    .doOnNext(d -> System.currentTimeMillis())
			    .map(d -> d)
			    .block()
		).satisfies(e -> assertThat(e.getSuppressed()[0])
				.hasMessageContaining("HooksTraceTest.java:")
				.hasMessageContaining("|_\tMono.map ⇢ reactor.HooksTraceTest.lambda$testTrace2$8(HooksTraceTest.java:")
		);
	}

	@Test
	public void testTrace3() {
		Hooks.onOperatorDebug();
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				Flux.just(1)
				    .map(d -> {
					    throw new RuntimeException();
				    })
				    .share()
				    .filter(d -> true)
				    .doOnNext(d -> System.currentTimeMillis())
				    .map(d -> d)
				    .blockLast()
		).satisfies(e -> assertThat(e.getSuppressed()[0])
				.hasMessageContaining("HooksTraceTest.java:")
				.hasMessageContaining("|_\tFlux.share ⇢ reactor.HooksTraceTest.lambda$testTrace3$14(HooksTraceTest.java:")
		);
	}

	@Test
	public void testTraceDefer() {
		Hooks.onOperatorDebug();
		try {
			//avoid wrapping this in yet another lambda (eg. AssertJ assertThatExceptionOfType().isThrownBy)
			//because in Java 8 at least it seems to cause the StackTraceElement to miss the method name: `lambda$null$xxx`
			Mono.defer(() -> Mono.just(1)
			                     .flatMap(d -> Mono.error(new IllegalStateException("boom")))
			                     .filter(d -> true)
			                     .doOnNext(d -> System.currentTimeMillis())
			                     .map(d -> d))
			    .block();
			fail("Expected IllegalStateException here");
		}
		catch (IllegalStateException ise) {
			assertThat(ise.getSuppressed()[0])
					.hasMessageContaining("HooksTraceTest.java:")
					.hasMessageContaining("|_\tMono.flatMap ⇢ reactor.HooksTraceTest.lambda$testTraceDefer$20(HooksTraceTest.java:");
		}
	}

	@Test
	public void testTraceComposed() {
		Hooks.onOperatorDebug();
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				Mono.just(1)
				    .flatMap(d -> Mono.error(new RuntimeException()))
				    .filter(d -> true)
				    .doOnNext(d -> System.currentTimeMillis())
				    .map(d -> d)
				    .block()
		).satisfies(e -> assertThat(e.getSuppressed()[0])
				.hasMessageContaining("HooksTraceTest.java:")
				.hasMessageContaining("|_\tMono.flatMap ⇢ reactor.HooksTraceTest.lambda$testTraceComposed$25(HooksTraceTest.java:")
		);
	}

	@Test
	public void testTraceComposed2() {
		Hooks.onOperatorDebug();
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				Flux.just(1)
				    .flatMap(d -> {
					    throw new RuntimeException();
				    })
				    .filter(d -> true)
				    .doOnNext(d -> System.currentTimeMillis())
				    .map(d -> d)
				    .blockLast()
		).satisfies(e -> assertThat(e.getSuppressed()[0])
				.hasMessageContaining("HooksTraceTest.java:")
				.hasMessageContaining("|_\tFlux.flatMap ⇢ reactor.HooksTraceTest.lambda$testTraceComposed2$31(HooksTraceTest.java:")
		);
	}

	@Test
	public void testOnLastPublisher() {
		List<Publisher> l = new ArrayList<>();
		Hooks.onLastOperator(p -> {
			System.out.println(Scannable.from(p).parents().count());
			System.out.println(Scannable.from(p).stepName());
			l.add(p);
			return p;
		});
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .map(m -> m)
		                        .takeUntilOther(Mono.never())
		                        .flatMap(d -> Mono.just(d).hide()))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(l).hasSize(5);
	}

	@Test
	public void testMultiReceiver() {
		Hooks.onOperatorDebug();
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

}
