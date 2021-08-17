/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * @author Simon Baslé
 */
@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ApiGroupingBenchmark {

	@Benchmark
	public void buffers_baseline(Blackhole bh) {
		bh.consume(PseudoFlux.range(1, 10)
			.buffer(4)
			.buffer(2)
		);
	}

	@Benchmark
	public void buffers_builderInstancePerCall(Blackhole bh) {
		bh.consume(PseudoFlux.range(1, 10)
			.buffers_v1()
			.fixedSize(4)
			.buffers_v1()
			.fixedSize(2)
		);
	}

	@Benchmark
	public void buffers_builderReusable(Blackhole bh) {
		bh.consume(PseudoFlux.range(1, 10)
			.buffers_v2()
			.fixedSize(4)
			.fixedSize(2)
			.generateFlux()
		);
	}

	@Benchmark
	public void buffers_specConsumer(Blackhole bh) {
		bh.consume(PseudoFlux.range(1, 10)
			.buffers_v3(configure -> configure.fixedSize(4).fixedSize(2))
		);
	}

	@Benchmark
	public void sideEffects_baseline(Blackhole bh) {
		bh.consume(PseudoFlux.range(1, 10)
			.doOnComplete(() -> bh.consume(1))
			.doOnNext(bh::consume)
		);
	}

	@Benchmark
	public void sideEffects_builderInstancePerCall(Blackhole bh) {
		bh.consume(PseudoFlux.range(1, 10)
			.sideEffects_v1()
			.doOnComplete(() -> bh.consume(1))
			.sideEffects_v1()
			.doOnNext(bh::consume)
		);
	}

	@Benchmark
	public void sideEffects_builderReusable(Blackhole bh) {
		bh.consume(PseudoFlux.range(1, 10)
			.sideEffects_v2()
			.doOnComplete(() -> bh.consume(1))
			.doOnNext(bh::consume)
			.endSideEffects()
		);
	}

	@Benchmark
	public void sideEffects_specConsumer(Blackhole bh) {
		bh.consume(PseudoFlux.range(1, 10)
			.sideEffects_v3(configure -> configure.doOnComplete(() -> bh.consume(1)).doOnNext(bh::consume))
		);
	}

	// === code to simulate the 3 options of API subgroups in a pseudo-Flux class ===

	private static abstract class PseudoFlux<T> {

		public static final PseudoFlux<Integer> range(int start, int count) {
			return new PseudoFlux<Integer>() {
				@Override
				public void invoke(Blackhole blackhole) {
					blackhole.consume(start);
					blackhole.consume(count);
				}
			};
		}

		public abstract void invoke(Blackhole blackhole);

		public final PseudoFlux<List<T>> buffer(int size) {
			return new PseudoFluxBuffer<>(this, size);
		}

		public final PseudoFlux<List<T>> buffer(int maxSize, Duration timeout) {
			return new PseudoFluxBuffer<>(this, maxSize, timeout);
		}

		public final PseudoFlux<T> doOnNext(Consumer<T> valueHandler) {
			return new PseudoFluxDo<>(this, valueHandler, null);
		}

		public final PseudoFlux<T> doOnComplete(Runnable completeHandler) {
			return new PseudoFluxDo<>(this, null, completeHandler);
		}

		public final FluxBuffersV1<T> buffers_v1() {
			return new FluxBuffersV1<T>(this);
		}

		public final FluxBuffersV2<T> buffers_v2() {
			return new FluxBuffersV2<T>(this);
		}

		public final <R> PseudoFlux<R> buffers_v3(Function<FluxBuffersV3<T>, FluxBuffersV3<R>> bufferSpec) {
			FluxBuffersV3<T> initialSpec = new FluxBuffersV3<T>(this);
			FluxBuffersV3<R> endSpec = bufferSpec.apply(initialSpec);
			return endSpec.generateFlux();
		}

		public final FluxSideEffectsV1<T> sideEffects_v1() {
			return new FluxSideEffectsV1<>(this);
		}

		public FluxSideEffectsV2<T> sideEffects_v2() {
			return new FluxSideEffectsV2<>(this);
		}

		public final PseudoFlux<T> sideEffects_v3(Consumer<FluxSideEffectsV3<T>> sideEffectSpec) {
			FluxSideEffectsV3<T> se = new FluxSideEffectsV3<T>(this);
			sideEffectSpec.accept(se);
			return se.generateFlux();
		}

	}

	private static final class PseudoFluxDo<T> extends PseudoFlux<T> {

		final PseudoFlux<T> source;
		final Consumer<T> valueHandler;
		final Runnable completeHandler;

		PseudoFluxDo(PseudoFlux<T> source, Consumer<T> valueHandler, Runnable completeHandler) {
			this.source = source;
			this.valueHandler = valueHandler;
			this.completeHandler = completeHandler;
		}

		@Override
		public void invoke(Blackhole blackhole) {
			blackhole.consume(source);
			blackhole.consume(valueHandler);
			blackhole.consume(completeHandler);
		}
	}

	private static final class PseudoFluxBuffer<T> extends PseudoFlux<List<T>> {

		final PseudoFlux<T> source;
		final int maxSize;
		final Duration timeout;

		PseudoFluxBuffer(PseudoFlux<T> source, int size) {
			this.source = source;
			this.maxSize = size;
			this.timeout = null;
		}

		PseudoFluxBuffer(PseudoFlux<T> source, int maxSize, Duration timeout) {
			this.source = source;
			this.maxSize = maxSize;
			this.timeout = timeout;
		}

		public void invoke(Blackhole blackhole) {
			blackhole.consume(source);
			blackhole.consume(maxSize);
			blackhole.consume(timeout);
		}
	}


	/*
		V1: one operator == one call to the api group

		PROS:
		 - simplest to use
		 - no juggling with generics: 1 call == 1 operator so we know the end return type when we get back to Flux
		 - no need for macro fusion friendliness for this category of operators

		CONS:
		 - one extra allocation per operator
		 - if formatter introduces newline before each dot, reads less like a typical reactive chain
	 */
	private static final class FluxBuffersV1<T> {

		final PseudoFlux<T> source;

		public FluxBuffersV1(PseudoFlux<T> source) {
			this.source = source;
		}

		public PseudoFlux<List<T>> fixedSize(int size) {
			return source.buffer(size);
		}

		public PseudoFlux<List<T>> sizeAndTimeout(int maxSize, Duration timeout) {
			return source.buffer(maxSize, timeout);
		}
	}

	/*
		V2: allow to set up multiple operators in a row then explicitly switch back to Flux API

		PROS:
		 - less allocations: allows to reuse api group instance to avoid 1 extra allocation per operator

		CONS:
		 - clunky termination of the api group needed to switch back to top level Flux API
		 - this is a pattern used nowhere else
		 - no clear benefit in enabling macro-fusion scenarios since these operators are not easily fuseable
	 */
	private static final class FluxBuffersV2<T> {

		final PseudoFlux<T> source;

		FluxBuffersV2(PseudoFlux<T> source) {
			this.source = source;
		}

		public FluxBuffersV2<List<T>> fixedSize(int size) {
			return new FluxBuffersV2<>(source.buffer(size));
		}

		public FluxBuffersV2<List<T>> sizeAndTimeout(int maxSize, Duration timeout) {
			return new FluxBuffersV2<>(source.buffer(maxSize, timeout));
		}

		public PseudoFlux<T> generateFlux() {
			return source;
		}
	}

	/*
		V3: allow to set up multiple operators in one api group call, materialized as a Consumer

		PROS:
		 - less allocations: allows to reuse api group instance to avoid 1 extra allocation per operator
		 - no need to look for a `endSideEffects()` method / remember to switch back to Flux
		 - this is an established pattern, notably in reactor-netty

		CONS:
		 - no benefit in terms of macro-fusion (these operators are not easily fused)
		 - no benefit in terms of logical grouping vs transform (less likely that somebody would want
			to provide a "standard" consumer that only deals with buffering)
	 */
	private static final class FluxBuffersV3<T> {

		final PseudoFlux<T> source;

		FluxBuffersV3(PseudoFlux<T> source) {
			this.source = source;
		}

		public FluxBuffersV3<List<T>> fixedSize(int size) {
			return new FluxBuffersV3<>(source.buffer(size));
		}

		public FluxBuffersV3<List<T>> sizeAndTimeout(int maxSize, Duration timeout) {
			return new FluxBuffersV3<>(source.buffer(maxSize, timeout));
		}

		PseudoFlux<T> generateFlux() {
			return source;
		}
	}

	/*
		V1: one operator == one call to the api group

		PROS:
		 - simplest to use

		CONS:
		 - one extra allocation per operator
		 - if formatter introduces newline before each dot, reads less like a typical reactive chain
		 - doesn't facilitate macro-fusion (although it is possible)
	 */
	private static final class FluxSideEffectsV1<T> {

		final PseudoFlux<T> source;

		FluxSideEffectsV1(PseudoFlux<T> source) {
			this.source = source;
		}

		public PseudoFlux<T> doOnNext(Consumer<T> nextHandler) {
			return source.doOnNext(nextHandler);
		}

		public PseudoFlux<T> doOnComplete(Runnable completeHandler) {
			return source.doOnComplete(completeHandler);
		}
	}

	/*
		V2: allow to set up multiple operators in a row then explicitly switch back to Flux API

		PROS:
		 - less allocations: allows to reuse api group instance to avoid 1 extra allocation per operator
		 - can facilitate advanced macro-fusion (since we have boundaries and can assume that none of these intermediate
		   steps will be used as independent Publisher)

		CONS:
		 - clunky termination of the api group needed to switch back to top level Flux API
		 - this is a pattern used nowhere else
	 */
	private static final class FluxSideEffectsV2<T> {

		PseudoFlux<T> built;

		FluxSideEffectsV2(PseudoFlux<T> source) {
			this.built = source;
		}

		public FluxSideEffectsV2<T> doOnNext(Consumer<T> nextHandler) {
			this.built = this.built.doOnNext(nextHandler);
			return this;
		}

		public FluxSideEffectsV2<T> doOnComplete(Runnable completeHandler) {
			this.built = this.built.doOnComplete(completeHandler);
			return this;
		}

		public PseudoFlux<T> endSideEffects() {
			return this.built;
		}
	}

	/*
		V3: allow to set up multiple operators in one api group call, materialized as a Consumer

		PROS:
		 - less allocations: allows to reuse api group instance to avoid 1 extra allocation per operator
		 - no need to look for a `endSideEffects()` method / remember to switch back to Flux
		 - this is an established pattern, notably in reactor-netty
		 - can facilitate advanced macro-fusion (since we have boundaries and can assume that none of these intermediate
		   steps will be used as independent Publisher)
		 - "standard" consumers (aka combinations of side effects) can be externalized

		CONS:
		 - arguably `it -> it` is the ugly part
	 */
	private static final class FluxSideEffectsV3<T> {

		PseudoFlux<T> built;

		FluxSideEffectsV3(PseudoFlux<T> source) {
			this.built = source;
		}

		public FluxSideEffectsV3<T> doOnNext(Consumer<T> nextHandler) {
			this.built = this.built.doOnNext(nextHandler);
			return this;
		}

		public FluxSideEffectsV3<T> doOnComplete(Runnable completeHandler) {
			this.built = this.built.doOnComplete(completeHandler);
			return this;
		}

		PseudoFlux<T> generateFlux() {
			return this.built;
		}
	}
}