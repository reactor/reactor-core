/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Collection;
import java.util.logging.Level;

import org.junit.Test;
import reactor.core.Fuseable;
import reactor.core.Fuseable.SynchronousSubscription;
import reactor.test.StepVerifier;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SignalLoggerTests {

	@Test
	public void testLogCollectionSubscription() {
		Flux<Integer> source = Flux.just(1, 2, 3);
		FluxPeekFuseable<Integer> flux = new FluxPeekFuseable<>(source, null, null, null, null, null, null, null);
		SignalLogger<Integer> signalLogger = new SignalLogger<>(flux,
				"test",
				Level.INFO,
				false, CollectionSpecialLogger::new,
				SignalType.ON_SUBSCRIBE);

		StepVerifier.create(flux.doOnSubscribe(signalLogger.onSubscribeCall()))
		            .expectSubscription()
		            .expectNext(1, 2, 3)
		            .expectComplete()
		            .verify();

		//verify that passing the subscription directly to logger would have considered
		// it a Collection and thus failed with this custom Logger.
		StepVerifier.create(flux.doOnSubscribe(s -> signalLogger.log(s, "")))
		            .expectErrorMatches(t -> t instanceof UnsupportedOperationException &&
				            t.getMessage().equals("Operators should not use this method!"))
		            .verify();
	}

	@Test
	public void nullSubscriptionAsString() {
		SignalLogger sl = new SignalLogger<>(Mono.empty(), null, null, false);

		assertThat(sl.subscriptionAsString(null), is("null subscription"));
	}

	@Test
	public void SynchronousSubscriptionAsString() {
		SignalLogger sl = new SignalLogger<>(Mono.empty(), null, null, false);
		SynchronousSubscription<Object> s = new FluxPeekFuseable.PeekFuseableSubscriber<>(null, null);

		assertThat(sl.subscriptionAsString(s), is("reactor.core.publisher.FluxPeekFuseable.PeekFuseableSubscriber, synchronous fuseable"));
	}
	@Test
	public void QueueSubscriptionAsString() {
		SignalLogger sl = new SignalLogger<>(Mono.empty(), null, null, false);
		Fuseable.QueueSubscription<Object> s = Operators.EmptySubscription.INSTANCE;

		assertThat(sl.subscriptionAsString(s), is("reactor.core.publisher.Operators.EmptySubscription, fuseable"));
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