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

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxOnAssembly.AssemblyLightSnapshotException;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshotException;
import reactor.util.context.Context;
import javax.annotation.Nullable;

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
		implements Fuseable, AssemblyOp, Scannable {

	final ParallelFlux<T>           source;
	final AssemblySnapshotException stacktrace;

	/**
	 * Create an assembly trace wrapping a {@link ParallelFlux}.
	 */
	ParallelFluxOnAssembly(ParallelFlux<T> source) {
		this.source = source;
		this.stacktrace = new AssemblySnapshotException();
	}

	/**
	 * Create an assembly trace augmented with a custom description (eg. a name for a
	 * ParallelFlux or a wider correlation ID), wrapping a {@link ParallelFlux}.
	 */
	ParallelFluxOnAssembly(ParallelFlux<T> source, @Nullable String description) {
		this.source = source;
		this.stacktrace = new AssemblySnapshotException(description);
	}

	/**
	 * Create a potentially light assembly trace augmented with a description (that must
	 * be unique enough to identify the assembly site in case of light mode),
	 * wrapping a {@link ParallelFlux}.
	 */
	ParallelFluxOnAssembly(ParallelFlux<T> source, String description, boolean light) {
		this.source = source;
		if (light) {
			 this.stacktrace = new AssemblyLightSnapshotException(description);
		}
		else {
			this.stacktrace = new AssemblySnapshotException(description);
		}
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T>[] subscribers, Context ctx) {
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

		source.subscribe(parents, ctx);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) return source;
		if (key == IntAttr.PREFETCH) return getPrefetch();

		return null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb = sb.append('{')
			   .append(" \"operator\" : ")
			   .append('"')
			   .append(getClass().getSimpleName()
								 .replaceAll("Flux", ""))
			   .append('"')
			   .append(", ")
		       .append(" \"description\" : ")
		       .append('"')
		       .append(stacktrace.getMessage())
		       .append('"')
		       .append(' ')
		       .append('}');
		return sb.toString();
	}
}