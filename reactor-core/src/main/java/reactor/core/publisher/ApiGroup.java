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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Simon Baslé
 */
public interface ApiGroup {

	final class FluxBuffersV1<T> implements ApiGroup {

		final Flux<T> source;

		public FluxBuffersV1(Flux<T> source) {
			this.source = source;
		}

		public Flux<List<T>> fixedSize(int size) {
			return Flux.onAssembly(new FluxBuffer<>(this.source, size, ArrayList::new));
		}

		public Flux<List<T>> sizeAndTimeout(int maxSize, Duration timeout) {
			return this.source.bufferTimeout(maxSize, timeout);
		}
	}

	final class FluxBuffersV2<T> {

		final Flux<T> source;

		FluxBuffersV2(Flux<T> source) {
			this.source = source;
		}

		public FluxBuffersV2<List<T>> fixedSize(int size) {
			return new FluxBuffersV2<>(this.source.buffer(size));
		}

		public FluxBuffersV2<List<T>> sizeAndTimeout(int maxSize, Duration timeout) {
			return new FluxBuffersV2<>(this.source.bufferTimeout(maxSize, timeout));
		}

		public Flux<T> generateFlux() {
			return source;
		}
	}

	final class FluxSideEffectsV2<T> {

		Flux<T> built;

		FluxSideEffectsV2(Flux<T> source) {
			this.built = source;
		}

		public FluxSideEffectsV2<T> log() {
			this.built = this.built.log();
			return this;
		}

		public FluxSideEffectsV2<T> doOnNext(Consumer<? super T> nextHandler) {
			this.built = this.built.doOnNext(nextHandler);
			return this;
		}

		public FluxSideEffectsV2<T> doOnComplete(Runnable completeHandler) {
			this.built = this.built.doOnComplete(completeHandler);
			return this;
		}

		public Flux<T> endSideEffects() {
			return this.built;
		}
	}

	public final class FluxSideEffectsV1<T> implements ApiGroup {

		final Flux<T> source;

		FluxSideEffectsV1(Flux<T> source) {
			this.source = source;
		}

		public Flux<T> log() {
			return source.log();
		}

		public Flux<T> doOnNext(Consumer<? super T> nextHandler) {
			return source.doOnNext(nextHandler);
		}

		public Flux<T> doOnComplete(Runnable completeHandler) {
			return source.doOnComplete(completeHandler);
		}
	}
}
