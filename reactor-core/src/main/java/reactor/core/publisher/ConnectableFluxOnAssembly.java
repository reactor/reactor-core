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

import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxOnAssembly.AssemblySnapshotException;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Captures the current stacktrace when this connectable publisher is created and
 * makes it available/visible for debugging purposes from
 * the inner Subscriber.
 * <p>
 * Note that getting a stacktrace is a costly operation.
 * <p>
 * The operator sanitizes the stacktrace and removes noisy entries such as:
 * <ul>
 * <li>java.lang.Thread entries</li>
 * <li>method references with source line of 1 (bridge methods)</li>
 * <li>Tomcat worker thread entries</li>
 * <li>JUnit setup</li>
 * </ul>
 * 
 * @param <T> the value type passing through
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class ConnectableFluxOnAssembly<T> extends ConnectableFlux<T> implements
                                                                    Fuseable, AssemblyOp,
                                                                    Scannable {

	final ConnectableFlux<T> source;

	final AssemblySnapshotException stacktrace;

	ConnectableFluxOnAssembly(ConnectableFlux<T> source) {
		this.source = source;
		this.stacktrace = new AssemblySnapshotException();
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		FluxOnAssembly.subscribe(s, source, stacktrace, ctx);
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		source.connect(cancelSupport);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == IntAttr.PREFETCH) return getPrefetch();
		if (key == ScannableAttr.PARENT) return source;

		return null;
	}
}