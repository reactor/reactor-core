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

import java.util.Collection;
import java.util.Objects;
import java.util.logging.Level;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.Fuseable.SynchronousSubscription;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.util.TestLogger;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class SignalLoggerTests {

	@Test
	public void safeLogsWhenLoggerThrows() {
		TestLogger logger = new TestLogger() {
			@Override
			public synchronized void info(String format, Object... arguments) {
				if (arguments[0] instanceof SignalType && arguments[1] instanceof Integer) {
					throw new UnsupportedOperationException("boom on integer");
				}
				super.info(format, arguments);
			}
		};

		SignalLogger signalLogger = new SignalLogger<>(Flux.empty(), null, Level.INFO, false, it -> logger);
		signalLogger.safeLog(SignalType.ON_NEXT, 404);

		assertThat(logger.getOutContent())
		          .contains("UnsupportedOperationException has been raised by the logging framework, does your log() placement make sense? " +
				          "eg. 'window(2).log()' instead of 'window(2).flatMap(w -> w.log())' - " +
				          "java.lang.UnsupportedOperationException: boom on integer")
		          .contains("onNext(404)");
	}

	@Test
	public void safeLogsWhenPassingQueueSubscription() {
		TestLogger logger = new TestLogger();

		SignalLogger signalLogger = new SignalLogger<>(Flux.empty(), null, Level.INFO, false, it -> logger);

		signalLogger.safeLog(SignalType.ON_NEXT, new FluxPeekFuseableTest.AssertQueueSubscription<>());

		assertThat(logger.getOutContent())
		          .contains("A Fuseable Subscription has been passed to the logging framework, this is generally a sign of a misplaced log(), " +
				          "eg. 'window(2).log()' instead of 'window(2).flatMap(w -> w.log())'")
		          .contains("onNext(reactor.core.publisher.FluxPeekFuseableTest$AssertQueueSubscription");
	}

	@Test
	public void testLogCollectionSubscription() {
		Flux<Integer> source = Flux.just(1, 2, 3);
		FluxPeekFuseable<Integer> flux = new FluxPeekFuseable<>(source, null, null, null, null, null, null, null);
		SignalLogger<Integer> signalLogger = new SignalLogger<>(flux,
				"test",
				Level.INFO,
				false, CollectionSpecialLogger::new,
				SignalType.ON_SUBSCRIBE);

		StepVerifier.create(flux.doOnSubscribe(Objects.requireNonNull(signalLogger.onSubscribeCall())))
		            .expectSubscription()
		            .expectNext(1, 2, 3)
		            .expectComplete()
		            .verify();

		//verify that passing the subscription directly to logger would have considered
		// it a Collection and thus failed with this custom Logger.
		StepVerifier.create(flux.doOnSubscribe(s -> signalLogger.log(SignalType.ON_SUBSCRIBE, s)))
		            .expectErrorMatches(t -> t instanceof UnsupportedOperationException &&
				            t.getMessage().equals(Fuseable.QueueSubscription.NOT_SUPPORTED_MESSAGE))
		            .verify();
	}

	@Test
	public void testLogQueueSubscriptionValue() {
		Flux<Flux<Integer>> source = Flux.just(1, 2, 3)
				.window(2); //windows happen to be UnicastProcessor, which implements QueueSubscription directly :o

		FluxPeek<Flux<Integer>> flux = new FluxPeek<>(source, null, null, null, null, null, null, null);
		SignalLogger<Flux<Integer>> signalLogger = new SignalLogger<>(flux,
				"test",
				Level.INFO,
				false, CollectionSpecialLogger::new,
				SignalType.ON_NEXT);

		StepVerifier.create(flux.doOnNext(Objects.requireNonNull(signalLogger.onNextCall())))
		            .expectNextCount(2)
		            .expectComplete()
		            .verify();

		//verify that passing the QueueSubscription directly to logger would have considered
		// it a Collection and thus failed with this custom Logger.
		StepVerifier.create(flux.doOnNext(w -> signalLogger.log(SignalType.ON_NEXT, w)))
		            .expectErrorMatches(t -> t instanceof UnsupportedOperationException &&
				            t.getMessage().equals(Fuseable.QueueSubscription.NOT_SUPPORTED_MESSAGE))
		            .verify();
	}

	@Test
	public void nullSubscriptionAsString() {
		assertThat(SignalLogger.subscriptionAsString(null)).isEqualTo("null subscription");
	}

	@Test
	public void normalSubscriptionAsString() {
		Subscription s = new FluxPeek.PeekSubscriber<>(null, null);

		assertThat(SignalLogger.subscriptionAsString(s)).isEqualTo("FluxPeek.PeekSubscriber");
	}

	@Test
	public void synchronousSubscriptionAsString() {
		SynchronousSubscription<Object> s = new FluxArray.ArraySubscription<>(null, null);

		assertThat(SignalLogger.subscriptionAsString(s)).isEqualTo("[Synchronous Fuseable] FluxArray.ArraySubscription");
	}

	@Test
	public void queueSubscriptionAsString() {
		Fuseable.QueueSubscription<Object> s = Operators.EmptySubscription.INSTANCE;

		assertThat(SignalLogger.subscriptionAsString(s)).isEqualTo("[Fuseable] Operators.EmptySubscription");
	}

	@Test
	public void anonymousSubscriptionAsString() {
		Subscription s = new Subscription() {
			@Override
			public void request(long n) {}

			@Override
			public void cancel() {}
		};

		assertThat(SignalLogger.subscriptionAsString(s)).isEqualTo("SignalLoggerTests$2");
	}

	@Test
	public void scanSignalLogger() {
		Mono<String> source = Mono.just("").map(i -> i);
		SignalLogger<String> sl = new SignalLogger<>(source, null, Level.INFO, false);

		assertThat(sl.scan(Scannable.Attr.PARENT)).isSameAs(source);
	}

	@Test
	public void logErrorUsesErrorWhenInfo() {
		Level level = Level.INFO;

		Mono<String> source = Mono.error(new IllegalStateException("ignored"));
		Logger mockLogger = Mockito.mock(Logger.class);
		when(mockLogger.isErrorEnabled()).thenReturn(true);
		when(mockLogger.isDebugEnabled()).thenReturn(true);
		when(mockLogger.isTraceEnabled()).thenReturn(true);

		SignalLogger<String> sl = new SignalLogger<>(source, null, level,
				false, s -> mockLogger);

		sl.onErrorCall().accept(new IllegalStateException("boom"));

		verify(mockLogger, times(1)).isErrorEnabled();
		verify(mockLogger, never()).isDebugEnabled();
		verify(mockLogger, never()).isTraceEnabled();
		verify(mockLogger, times(1)).error(Mockito.anyString(), (Object[]) Mockito.any());
		verify(mockLogger, times(1)).error(Mockito.anyString(), (Throwable) Mockito.any());
		verifyNoMoreInteractions(mockLogger);
	}

	@Test
	public void logErrorUsesErrorWhenWarning() {
		Level level = Level.WARNING;

		Mono<String> source = Mono.error(new IllegalStateException("ignored"));
		Logger mockLogger = Mockito.mock(Logger.class);
		when(mockLogger.isErrorEnabled()).thenReturn(true);
		when(mockLogger.isDebugEnabled()).thenReturn(true);
		when(mockLogger.isTraceEnabled()).thenReturn(true);

		SignalLogger<String> sl = new SignalLogger<>(source, null, level,
				false, s -> mockLogger);

		sl.onErrorCall().accept(new IllegalStateException("boom"));

		verify(mockLogger, times(1)).isErrorEnabled();
		verify(mockLogger, never()).isDebugEnabled();
		verify(mockLogger, never()).isTraceEnabled();
		verify(mockLogger, times(1)).error(Mockito.anyString(), (Object[]) Mockito.any());
		verify(mockLogger, times(1)).error(Mockito.anyString(), (Throwable) Mockito.any());
		verifyNoMoreInteractions(mockLogger);
	}

	@Test
	public void logErrorUsesErrorWhenSevere() {
		Level level = Level.SEVERE;

		Mono<String> source = Mono.error(new IllegalStateException("ignored"));
		Logger mockLogger = Mockito.mock(Logger.class);
		when(mockLogger.isErrorEnabled()).thenReturn(true);
		when(mockLogger.isDebugEnabled()).thenReturn(true);
		when(mockLogger.isTraceEnabled()).thenReturn(true);

		SignalLogger<String> sl = new SignalLogger<>(source, null, level,
				false, s -> mockLogger);

		sl.onErrorCall().accept(new IllegalStateException("boom"));

		verify(mockLogger, times(1)).isErrorEnabled();
		verify(mockLogger, never()).isDebugEnabled();
		verify(mockLogger, never()).isTraceEnabled();
		verify(mockLogger, times(1)).error(Mockito.anyString(), (Object[]) Mockito.any());
		verify(mockLogger, times(1)).error(Mockito.anyString(), (Throwable) Mockito.any());
		verifyNoMoreInteractions(mockLogger);
	}

	@Test
	public void logErrorUsesDebugWhenFine() {
		Level level = Level.FINE;

		Mono<String> source = Mono.error(new IllegalStateException("ignored"));
		Logger mockLogger = Mockito.mock(Logger.class);
		when(mockLogger.isErrorEnabled()).thenReturn(true);
		when(mockLogger.isDebugEnabled()).thenReturn(true);
		when(mockLogger.isTraceEnabled()).thenReturn(true);

		SignalLogger<String> sl = new SignalLogger<>(source, null, level,
				false, s -> mockLogger);

		sl.onErrorCall().accept(new IllegalStateException("boom"));

		verify(mockLogger, never()).isErrorEnabled();
		verify(mockLogger, times(1)).isDebugEnabled();
		verify(mockLogger, never()).isTraceEnabled();
		verify(mockLogger, times(1)).debug(Mockito.anyString(), (Object[]) Mockito.any());
		verify(mockLogger, times(1)).debug(Mockito.anyString(), (Throwable) Mockito.any());
		verifyNoMoreInteractions(mockLogger);
	}

	@Test
	public void logErrorUsesTraceWhenFinest() {
		Level level = Level.FINEST;
		demonstrateLogError(); //added to the test suite so that sanity check can be done

		Mono<String> source = Mono.error(new IllegalStateException("ignored"));
		Logger mockLogger = Mockito.mock(Logger.class);
		when(mockLogger.isErrorEnabled()).thenReturn(true);
		when(mockLogger.isDebugEnabled()).thenReturn(true);
		when(mockLogger.isTraceEnabled()).thenReturn(true);

		SignalLogger<String> sl = new SignalLogger<>(source, null, level,
				false, s -> mockLogger);

		sl.onErrorCall().accept(new IllegalStateException("boom"));

		verify(mockLogger, never()).isErrorEnabled();
		verify(mockLogger, never()).isDebugEnabled();
		verify(mockLogger, times(1)).isTraceEnabled();
		verify(mockLogger, times(1)).trace(Mockito.anyString(), (Object[]) Mockito.any());
		verify(mockLogger, times(1)).trace(Mockito.anyString(), (Throwable) Mockito.any());
		verifyNoMoreInteractions(mockLogger);
	}

	@Test
	public void fluxLogWithGivenLogger() {
		Level level = Level.WARNING;

		Flux<String> source = Flux.just("foo");
		Logger mockLogger = Mockito.mock(Logger.class);

		source.log(mockLogger, level, false, SignalType.ON_NEXT)
		      .subscribe();

		verify(mockLogger, only()).warn(anyString(), eq(SignalType.ON_NEXT),
				eq("foo"));
		verifyNoMoreInteractions(mockLogger);
	}

	@Test
	public void monoLogWithGivenLogger() {
		Level level = Level.WARNING;

		Mono<String> source = Mono.just("foo");
		Logger mockLogger = Mockito.mock(Logger.class);

		source.log(mockLogger, level, false, SignalType.ON_NEXT)
		      .subscribe();

		verify(mockLogger, only()).warn(anyString(), eq(SignalType.ON_NEXT),
				eq("foo"));
		verifyNoMoreInteractions(mockLogger);
	}

	@Test
	public void noCurrentContextLogWhenInfo() {
		SignalLogger<?> signalLogger = new SignalLogger<>(Mono.empty(), null,
				Level.INFO, false);

		assertThat(signalLogger.onCurrentContextCall()).isNull();

	}

	@Test
	public void currentContextLogWhenDebug() {
		TestLogger logger = new TestLogger();

		SignalLogger<?> signalLogger = new SignalLogger<>(Mono.empty(), null,
				Level.FINE,false, name -> logger);

		assertThat(logger.getOutContent()).as("before currentContext()").isEmpty();

		Context context = Context.of("foo", "bar");
		signalLogger.onCurrentContextCall().accept(context);

		assertThat(logger.getOutContent())
				.startsWith("[DEBUG] (")
				.endsWith(") currentContext(Context1{foo=bar})\n");
	}

	@Test
	public void currentContextLogWhenTrace() {
		TestLogger logger = new TestLogger();

		SignalLogger<?> signalLogger = new SignalLogger<>(Mono.empty(), null,
				Level.FINEST,false, name -> logger);

		assertThat(logger.getOutContent()).as("before currentContext()").isEmpty();

		Context context = Context.of("foo", "bar");
		signalLogger.onCurrentContextCall().accept(context);

		assertThat(logger.getOutContent())
				.startsWith("[TRACE] (")
				.endsWith(") currentContext(Context1{foo=bar})\n");
	}

	private void demonstrateLogError() {
		Loggers.getLogger("logError.default")
		       .warn("The following logs should demonstrate similar error output, but respectively at ERROR, DEBUG and TRACE levels");
		Mono<Object> error = Mono.error(new IllegalStateException("boom"));

		error.log("logError.default")
		     .subscribe(v -> {}, e -> {});

		error.log("logError.fine", Level.FINE)
		     .subscribe(v -> {}, e -> {});

		error.log("logError.finest", Level.FINEST)
		     .subscribe(v -> {}, e -> {});
	}

	//=========================================================
	//A Logger that will transform any Collection argument into
	//a string representation of its iterator
	//=========================================================
	private static class CollectionSpecialLogger implements Logger {

		private final Logger delegate;

		public CollectionSpecialLogger(String name) {
			this.delegate = Loggers.getLogger(name);
		}

		private Object collectionAsString(Object source) {
			if (!(source instanceof Collection)) {
				return source;
			}
			Collection<?> c = (Collection) source;
			StringBuilder str = new StringBuilder();
			str.append('[');
			boolean isFirst = true;
			for (final Object o : c) {
				if (isFirst) {
					isFirst = false;
				} else {
					str.append(", ");
				}
				str.append(o);
			}
			str.append(']');
			return str.toString();
		}

		private Object[] wrapArguments(Object... arguments) {
			Object[] args = new Object[arguments.length];
			for (int i = 0; i < arguments.length; i++) {
				args[i] = collectionAsString(arguments[i]);
			}
			return args;
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		public boolean isTraceEnabled() {
			return true;
		}

		@Override
		public void trace(String msg) {
			delegate.trace(msg);
		}

		@Override
		public void trace(String format, Object... arguments) {
			delegate.trace(format, wrapArguments(arguments));
		}

		@Override
		public void trace(String msg, Throwable t) {
			delegate.trace(msg, t);
		}

		@Override
		public boolean isDebugEnabled() {
			return delegate.isDebugEnabled();
		}

		@Override
		public void debug(String msg) {
			delegate.debug(msg);
		}

		@Override
		public void debug(String format, Object... arguments) {
			delegate.debug(format, wrapArguments(arguments));
		}

		@Override
		public void debug(String msg, Throwable t) {
			delegate.debug(msg, t);
		}

		@Override
		public boolean isInfoEnabled() {
			return delegate.isInfoEnabled();
		}

		@Override
		public void info(String msg) {
			delegate.info(msg);
		}

		@Override
		public void info(String format, Object... arguments) {
			delegate.info(format, wrapArguments(arguments));
		}

		@Override
		public void info(String msg, Throwable t) {
			delegate.info(msg, t);
		}

		@Override
		public boolean isWarnEnabled() {
			return delegate.isWarnEnabled();
		}

		@Override
		public void warn(String msg) {
			delegate.warn(msg);
		}

		@Override
		public void warn(String format, Object... arguments) {
			delegate.warn(format, wrapArguments(arguments));
		}

		@Override
		public void warn(String msg, Throwable t) {
			delegate.warn(msg, t);
		}

		@Override
		public boolean isErrorEnabled() {
			return delegate.isErrorEnabled();
		}

		@Override
		public void error(String msg) {
			delegate.error(msg);
		}

		@Override
		public void error(String format, Object... arguments) {
			delegate.error(format, wrapArguments(arguments));
		}

		@Override
		public void error(String msg, Throwable t) {
			delegate.error(msg, t);
		}
	}

}