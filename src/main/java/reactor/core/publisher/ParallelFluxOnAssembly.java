/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Receiver;

/**
 * Captures the current stacktrace when this connectable publisher is created and makes it
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
final class ParallelFluxOnAssembly<T> extends ParallelFlux<T>
		implements Fuseable, AssemblyOp, Receiver {

	final ParallelFlux<T>                                                          source;

	final Exception stacktrace;

	ParallelFluxOnAssembly(ParallelFlux<T> source) {
		this.source = source;
		this.stacktrace = new Exception();
	}

	@Override
	public long getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}

		int n = subscribers.length;
		@SuppressWarnings("unchecked") Subscriber<? super T>[] parents =
				new Subscriber[n];
		Subscriber<? super T> s;
		for (int i = 0; i < n; i++) {
			s = subscribers[i];
			if (s instanceof ConditionalSubscriber) {
				ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<?
						super T>) s;
				s = new FluxOnAssembly.OnAssemblyConditionalSubscriber<>(cs,
						stacktrace,
						source);
			}
			else {
				s = new FluxOnAssembly.OnAssemblySubscriber<>(s, stacktrace, source);
			}
			parents[i] = s;
		}

		source.subscribe(parents);
	}

	@Override
	public Object upstream() {
		return source;
	}
}