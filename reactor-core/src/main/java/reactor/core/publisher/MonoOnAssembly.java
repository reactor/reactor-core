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
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class MonoOnAssembly<T> extends MonoOperator<T, T> implements Fuseable, AssemblyOp {

	final AssemblySnapshotException stacktrace;

	/**
	 * Create an assembly trace exposed as a {@link Mono}.
	 */
	MonoOnAssembly(Mono<? extends T> source) {
		super(source);
		this.stacktrace = new AssemblySnapshotException();
	}

	/**
	 * Create an assembly trace augmented with a custom description (eg. a name for a Mono
	 * or a wider correlation ID) and exposed as a {@link Mono}.
	 */
	MonoOnAssembly(Mono<? extends T> source, @Nullable String description) {
		super(source);
		this.stacktrace = new AssemblySnapshotException(description);
	}

	/**
	 * Create a potentially light assembly trace augmented with a description (that must
	 * be unique enough to identify the assembly site in case of light mode),
	 * wrapping a {@link ParallelFlux}.
	 */
	MonoOnAssembly(Mono<? extends T> source, String description, boolean light) {
		super(source);
		if (light) {
			this.stacktrace = new FluxOnAssembly.AssemblyLightSnapshotException(description);
		}
		else {
			this.stacktrace = new AssemblySnapshotException(description);
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		FluxOnAssembly.subscribe(s, source, stacktrace, ctx);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb = sb.append('{')
			   .append(" \"operator\" : ")
			   .append('"')
			   .append(getClass().getSimpleName()
								 .replaceAll("Mono", ""))
			   .append('"');
		if (stacktrace != null) {
			sb = sb.append(", ")
				   .append(" \"description\" : ")
				   .append('"')
				   .append(stacktrace.getMessage())
				   .append('"');
		}
		return sb.append(' ')
				 .append('}')
				 .toString();
	}
}
