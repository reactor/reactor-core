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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.Exceptions;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.EmissionException;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assumptions.assumeThat;

class InternalOneSinkTest {

	@ParameterizedTest
	@EnumSource(value = Sinks.EmitResult.class)
	void shouldDelegateToHandler(EmitResult emitResult) {
		assumeThat(emitResult.isFailure()).isTrue();
		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {
			@Override
			public Sinks.EmitResult tryEmitValue(Object value) {
				return emitResult;
			}

			@Override
			public Sinks.EmitResult tryEmitError(Throwable error) {
				return emitResult;
			}

			@Override
			public Sinks.EmitResult tryEmitEmpty() {
				return emitResult;
			}
		};

		for (SignalType signalType : new SignalType[] {SignalType.ON_NEXT, SignalType.ON_ERROR, SignalType.ON_COMPLETE}) {
			AtomicReference<Sinks.EmitResult> emissionRef = new AtomicReference<>();
			try {
				EmitFailureHandler handler = (failedSignalType, failedEmission) -> {
					emissionRef.set(failedEmission);
					return false;
				};
				switch (signalType) {
					case ON_NEXT:
						sink.emitValue("Hello", handler);
						break;
					case ON_ERROR:
						sink.emitError(new Exception("boom"), handler);
						break;
					case ON_COMPLETE:
						sink.emitEmpty(handler);
						break;
					default:
						throw new IllegalStateException();
				}
			}
			catch (EmissionException e) {
				assertThat(e.getReason()).isEqualTo(Sinks.EmitResult.FAIL_NON_SERIALIZED);
			}
			assertThat(emissionRef).as("emitResult").hasValue(emitResult);
		}
	}

	@Test
	void shouldRetry() {
		AtomicReference<Sinks.EmitResult> nextEmission = new AtomicReference<>(EmitResult.FAIL_NON_SERIALIZED);

		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {

			@Override
			public EmitResult tryEmitValue(Object value) {
				return nextEmission.get();
			}

			@Override
			public Sinks.EmitResult tryEmitEmpty() {
				throw new IllegalStateException();
			}

			@Override
			public Sinks.EmitResult tryEmitError(Throwable error) {
				throw new IllegalStateException();
			}
		};

		assertThatExceptionOfType(EmissionException.class).isThrownBy(() -> {
			sink.emitValue("Hello", (signalType, emission) -> {
				nextEmission.set(Sinks.EmitResult.OK);
				return false;
			});
		});

		nextEmission.set(EmitResult.FAIL_NON_SERIALIZED);
		sink.emitValue("Hello", (signalType, emission) -> {
			nextEmission.set(Sinks.EmitResult.OK);
			return true;
		});
	}


	@Test
	void shouldRethrowNonSerializedEmission() {
		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {
			@Override
			public EmitResult tryEmitValue(Object o) {
				return Sinks.EmitResult.FAIL_NON_SERIALIZED;
			}
		};

		assertThatExceptionOfType(EmissionException.class).isThrownBy(() -> {
			sink.emitValue("Hello", EmitFailureHandler.FAIL_FAST);
		});
	}

	@Test
	void shouldFailOnOverflow() {
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {
			@Override
			public Sinks.EmitResult tryEmitValue(Object o) {
				return Sinks.EmitResult.FAIL_OVERFLOW;
			}

			@Override
			public EmitResult tryEmitError(Throwable error) {
				errorRef.set(error);
				return Sinks.EmitResult.OK;
			}
		};

		sink.emitValue("Hello", EmitFailureHandler.FAIL_FAST);
		assertThat(errorRef.get()).as("error").matches(Exceptions::isOverflow);
	}

	@Test
	void shouldRetryUntilExitCondition() {
		AtomicInteger i = new AtomicInteger();
		Sinks.One<Object> sink = new InternalOneSinkAdapter<Object>() {

			@Override
			public Sinks.EmitResult tryEmitValue(Object o) {
				return i.incrementAndGet() == 5 ? Sinks.EmitResult.OK : Sinks.EmitResult.FAIL_NON_SERIALIZED;
			}

			@Override
			public Sinks.EmitResult tryEmitEmpty() {
				throw new IllegalStateException();
			}

			@Override
			public EmitResult tryEmitError(Throwable error) {
				throw new IllegalStateException();
			}
		};

		sink.emitValue("Hello", (signalType, emission) -> true);
		assertThat(i).hasValue(5);
	}

	static class InternalOneSinkAdapter<T> implements InternalOneSink<T> {

		@Override
		public Context currentContext() {
			return Context.empty();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}

		@Override
		public Sinks.EmitResult tryEmitValue(T value) {
			return Sinks.EmitResult.OK;
		}

		@Override
		public Sinks.EmitResult tryEmitError(Throwable error) {
			return Sinks.EmitResult.OK;
		}

		@Override
		public Sinks.EmitResult tryEmitEmpty() {
			return Sinks.EmitResult.OK;
		}

		@Override
		public int currentSubscriberCount() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Mono<T> asMono() {
			throw new UnsupportedOperationException();
		}
	}
}