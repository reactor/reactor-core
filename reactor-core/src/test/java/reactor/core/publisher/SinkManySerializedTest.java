/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(5)
public class SinkManySerializedTest {

	@Test
	public void shouldNotThrowFromTryEmitNext() {
		SinkManySerialized<Object> sink = new SinkManySerialized<>(
				new EmptyMany<>(),
				Context::empty
		);

		StepVerifier.create(sink.asFlux(), 0)
		            .expectSubscription()
		            .then(() -> {
			            assertThat(sink.tryEmitNext("boom"))
					            .as("emission")
					            .isEqualTo(EmitResult.FAIL_OVERFLOW);
		            })
		            .then(() -> sink.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	public void shouldSignalErrorOnOverflow() {
		SinkManySerialized<Object> sink = new SinkManySerialized<>(
				new EmptyMany<>(),
				Context::empty
		);

		StepVerifier.create(sink.asFlux(), 0)
		            .expectSubscription()
		            .then(() -> sink.emitNext("boom", Sinks.EmitFailureHandler.FAIL_FAST))
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void shouldReturnTheEmission() {
		AtomicReference<SinkManySerialized<Object>> sink = new AtomicReference<>();
		sink.set(
				new SinkManySerialized<>(
						new EmptyMany<Object>() {
							@Override
							public EmitResult tryEmitNext(Object o) {
								SinkManySerialized.LOCKED_AT.set(sink.get(), new Thread());
								return super.tryEmitNext(o);
							}
						},
						Context::empty
				)
		);

		assertThat(sink.get().tryEmitNext("boom"))
				.as("emission")
				.isEqualTo(Sinks.EmitResult.FAIL_OVERFLOW);
	}

	@Test
	public void sameThreadRecursion() throws Exception {
		Sinks.Many<Object> sink = Sinks.unsafe().many().multicast().directBestEffort();
		SinkManySerialized<Object> manySink = new SinkManySerialized<>(sink, Context::empty);

		CompletableFuture<Object> muchFuture = sink.asFlux().doOnNext(o -> {
			if ("first".equals(o)) {
				assertThat(manySink.lockedAt).as("lockedAt").isEqualTo(Thread.currentThread());
				assertThat(manySink.tryEmitNext("second")).as("second").isEqualTo(
						EmitResult.OK);
			}
		}).next().toFuture();

		assertThat(manySink.tryEmitNext("first")).as("first").isEqualTo(EmitResult.OK);
		muchFuture.get(5, TimeUnit.SECONDS);
	}

	static class EmptyMany<T> implements Sinks.Many<T> {

		final Sinks.Many<T> delegate = Sinks.unsafe().many().multicast().directBestEffort();

		@Override
		public EmitResult tryEmitNext(T o) {
			return EmitResult.FAIL_OVERFLOW;
		}

		@Override
		public EmitResult tryEmitComplete() {
			return delegate.tryEmitComplete();
		}

		@Override
		public EmitResult tryEmitError(Throwable error) {
			return delegate.tryEmitError(error);
		}

		@Override
		public void emitNext(T t, Sinks.EmitFailureHandler failureHandler) {
			throw new IllegalStateException("Not expected to be called");
		}

		@Override
		public void emitComplete(Sinks.EmitFailureHandler failureHandler) {
			delegate.emitComplete(failureHandler);
		}

		@Override
		public void emitError(Throwable error, Sinks.EmitFailureHandler failureHandler) {
			delegate.emitError(error, failureHandler);
		}

		@Override
		public int currentSubscriberCount() {
			return delegate.currentSubscriberCount();
		}

		@Override
		public Flux<T> asFlux() {
			return delegate.asFlux();
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return delegate.inners();
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			return delegate.scanUnsafe(key);
		}
	}
}