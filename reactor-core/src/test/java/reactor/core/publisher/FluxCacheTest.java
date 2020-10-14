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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxCacheTest {

	@Test
	public void cacheFlux() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000)
				                                         , vts)
		                                         .cache()
		                                         .elapsed(vts);

		StepVerifier.withVirtualTime(() -> source, () -> vts, Long.MAX_VALUE)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.withVirtualTime(() -> source, () -> vts, Long.MAX_VALUE)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();
	}

	@Test
	public void cacheFluxTTL() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000)
				                                         , vts)
		                                         .cache(Duration.ofMillis(2000), vts)
		                                         .elapsed(vts);

		StepVerifier.withVirtualTime(() -> source, () -> vts, Long.MAX_VALUE)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.withVirtualTime(() -> source, () -> vts, Long.MAX_VALUE)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();
	}

	@Test
	public void cacheFluxHistoryTTL() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis
				                                         (1000), vts)
		                                         .cache(2, Duration.ofMillis(2000), vts)
		                                         .elapsed(vts);

		StepVerifier.withVirtualTime(() -> source, () -> vts, Long.MAX_VALUE)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.withVirtualTime(() -> source, () -> vts, Long.MAX_VALUE)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxTTL2() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		AtomicInteger i = new AtomicInteger(0);
		Flux<Integer> source = Flux.defer(() -> Flux.just(i.incrementAndGet()))
		                           .cache(Duration.ofMillis(2000), vts);

		StepVerifier.create(source)
		            .expectNext(1)
		            .verifyComplete();

		StepVerifier.create(source)
		            .expectNext(1)
		            .verifyComplete();

		vts.advanceTimeBy(Duration.ofSeconds(3));

		StepVerifier.create(source)
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void cacheContextHistory() {
		AtomicInteger contextFillCount = new AtomicInteger();
		Flux<String> cached = Flux.just(1, 2)
		                          .flatMap(i -> Mono.deferContextual(Mono::just)
		                                            .map(ctx -> ctx.getOrDefault("a", "BAD"))
		                          )
		                          .cache(1)
		                          .contextWrite(ctx -> ctx.put("a", "GOOD" + contextFillCount.incrementAndGet()));

		//at first pass, the context is captured
		String cacheMiss = cached.blockLast();
		assertThat(cacheMiss).as("cacheMiss").isEqualTo("GOOD1");
		assertThat(contextFillCount).as("cacheMiss").hasValue(1);

		//at second subscribe, the Context fill attempt is still done, but ultimately ignored since first context is cached
		String cacheHit = cached.blockLast();
		assertThat(cacheHit).as("cacheHit").isEqualTo("GOOD1"); //value from the cache
		assertThat(contextFillCount).as("cacheHit").hasValue(2); //function was still invoked

		//at third subscribe, function is called for the 3rd time, but the context is still cached
		String cacheHit2 = cached.blockLast();
		assertThat(cacheHit2).as("cacheHit2").isEqualTo("GOOD1");
		assertThat(contextFillCount).as("cacheHit2").hasValue(3);

		//at fourth subscribe, function is called for the 4th time, but the context is still cached
		String cacheHit3 = cached.blockLast();
		assertThat(cacheHit3).as("cacheHit3").isEqualTo("GOOD1");
		assertThat(contextFillCount).as("cacheHit3").hasValue(4);
	}

	@Test
	public void cacheContextTime() {
		AtomicInteger contextFillCount = new AtomicInteger();

		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		Flux<String> cached = Flux.just(1)
		                          .flatMap(i -> Mono.deferContextual(Mono::just)
		                                            .map(ctx -> ctx.getOrDefault("a", "BAD"))
		                          )
		                          .replay(Duration.ofMillis(500), vts)
		                          .autoConnect()
		                          .contextWrite(ctx -> ctx.put("a", "GOOD" + contextFillCount.incrementAndGet()));

		//at first pass, the context is captured
		String cacheMiss = cached.blockLast();
		assertThat(cacheMiss).as("cacheMiss").isEqualTo("GOOD1");
		assertThat(contextFillCount).as("cacheMiss").hasValue(1);

		//at second subscribe, the Context fill attempt is still done, but ultimately ignored since Mono.deferContextual(Mono::just) result is cached
		String cacheHit = cached.blockLast();
		assertThat(cacheHit).as("cacheHit").isEqualTo("GOOD1"); //value from the cache
		assertThat(contextFillCount).as("cacheHit").hasValue(2); //function was still invoked

		vts.advanceTimeBy(Duration.ofMillis(501));

		//at third subscribe, after the expiration delay, function is called for the 3rd time, but this time the resulting context is cached
		String cacheExpired = cached.blockLast();
		assertThat(cacheExpired).as("cacheExpired").isEqualTo("GOOD3");
		assertThat(contextFillCount).as("cacheExpired").hasValue(3);

		//at fourth subscribe, function is called but ignored, the cached context is visible
		String cachePostExpired = cached.blockLast();
		assertThat(cachePostExpired).as("cachePostExpired").isEqualTo("GOOD3");
		assertThat(contextFillCount).as("cachePostExpired").hasValue(4);

		vts.dispose();
	}

}
