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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;

/**
 * @author Simon Baslé
 */
public interface ApiGroup {

	/**
	 * An {@link ApiGroup} that is explicitly <strong>combinable</strong>, allowing to set up
	 * several operators in a row as long as all these operators maintain the original {@link Flux}
	 * or {@link Mono} type.
	 * <p>
	 * In other words, instead of immediately returning a {@link Flux} or {@link Mono}, its methods
	 * add new operators to the chain until {@link #apply()} is invoked (at which point
	 * the outermost operator instance is returned).
	 *
	 * @param <T> the type of data
	 * @param <P>
	 */
	interface Combinable<T, P extends Publisher<T>> extends ApiGroup {
		P apply();
	}

	static <T> Buffers<T> bufferOf(Flux<T> source) {
		return new Buffers<>(source);
	}

	static <T> FluxSideEffects<T> sideEffects(Flux<T> source) {
		return new FluxSideEffects<>(source);
	}

	final class Buffers<T> implements ApiGroup {

		final Flux<T> source;

		public Buffers(Flux<T> source) {
			this.source = source;
		}

		public Flux<List<T>> fixedSize(int size) {
			return source.onAssembly(new FluxBuffer<>(this.source, size, ArrayList::new));
		}
	}

	final class FluxSideEffects<T> implements ApiGroup.Combinable<T, Flux<T>> {

		Flux<T> built;

		public FluxSideEffects(Flux<T> source) {
			this.built = source;
		}

		FluxSideEffects<T> log() {
			this.built = this.built.log();
			return this;
		}

		FluxSideEffects<T> doOnNext(Consumer<? super T> nextHandler) {
			this.built = this.built.doOnNext(nextHandler);
			return this;
		}

		FluxSideEffects<T> doOnComplete(Runnable completeHandler) {
			this.built = this.built.doOnComplete(completeHandler);
			return this;
		}

		@Override
		public Flux<T> apply() {
			return this.built;
		}
	}
}
