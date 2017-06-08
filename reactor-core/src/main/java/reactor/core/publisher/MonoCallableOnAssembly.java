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

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.Callable;

import org.reactivestreams.Subscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshotException;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Captures the current stacktrace when this publisher is created and makes it
 * available/visible for debugging purposes from the inner Subscriber.
 * <p>
 * Note that getting a stacktrace is a costly operation.
 * <p>
 * The operator sanitizes the stacktrace and removes noisy entries such as: <ul>
 * <li>java.lang.Thread entries</li> <li>method references with source line of 1 (bridge
 * methods)</li> <li>Tomcat worker thread entries</li> <li>JUnit setup</li> </ul>
 *
 * @param <T> the value type passing through
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class MonoCallableOnAssembly<T> extends MonoOperator<T, T>
		implements Callable<T>, AssemblyOp {

	final AssemblySnapshotException stacktrace;

	MonoCallableOnAssembly(Mono<? extends T> source) {
		super(source);
		this.stacktrace = new AssemblySnapshotException();
	}

	@Override
	@Nullable
	public T block() {
		//duration is ignored below
		return block(Duration.ZERO);
	}

	@Override
	@Nullable
	@SuppressWarnings("unchecked")
	public T block(Duration timeout) {
		try {
			return ((Callable<T>) source).call();
		}
		catch (Throwable e) {
			throw Exceptions.propagate(e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		FluxOnAssembly.subscribe(s, source, stacktrace, ctx);
	}

	@SuppressWarnings("unchecked")
	@Override
	@Nullable
	public T call() throws Exception {
		return ((Callable<T>) source).call();
	}
}
