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

import java.util.concurrent.Callable;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshot;

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
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxCallableOnAssembly<T> extends InternalFluxOperator<T, T>
		implements Fuseable, Callable<T>, AssemblyOp {

	final AssemblySnapshot stacktrace;

	FluxCallableOnAssembly(Flux<? extends T> source, AssemblySnapshot stacktrace) {
		super(source);
		this.stacktrace = stacktrace;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return FluxOnAssembly.wrapSubscriber(actual, source, stacktrace);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T call() throws Exception {
		return ((Callable<T>) source).call();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.ACTUAL_METADATA) return !stacktrace.checkpointed;

		return super.scanUnsafe(key);
	}

	@Override
	public String stepName() {
		return stacktrace.operatorAssemblyInformation();
	}

	@Override
	public String toString() {
		return stacktrace.operatorAssemblyInformation();
	}
}
