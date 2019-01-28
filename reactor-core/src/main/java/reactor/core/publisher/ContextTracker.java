/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
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

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.annotation.NonNull;
import reactor.util.context.Context;

/**
 * A set of callbacks to track the {@link Context} propagation inside a reactive execution
 * chain.
 *
 * @author Sergei Egorov
 * @since 3.2.6
 */
public interface ContextTracker {

	/**
	 * A callback to be triggered on the subscription.
	 * Implementors should return a new, enriched {@link Context} (see {@link Context#put(Object, Object)}),
	 * or the same context if the tracking is not desired.
	 * <p>
	 * Example implementation:
	 * <pre>{@code
	 * if (!shouldTrack()) return context;
	 * Marker parent = context.getOrDefault(Marker.class, null);
	 * Marker marker = new Marker(parent);
	 * return context.put(Marker.class, marker);
	 * }</pre>
	 *
	 * @param context the {@link Context} of {@link reactor.core.CoreSubscriber}
	 * @see reactor.core.CoreSubscriber
	 * @return modified or the same {@link Context}
	 */
	@NonNull
	Context onSubscribe(Context context);

	/**
	 * {@link ContextTracker} implementors receive this callback before
	 * the {@link reactor.core.scheduler.Scheduler}s execute scheduled {@link Runnable}s.
	 * <p>
	 * The method would extract tracking information from {@link Context} populated in {@link #onSubscribe(Context)}
	 * and put it in appropriate {@link ThreadLocal} variables,
	 * then return a {@link Disposable} that clears said {@link ThreadLocal}s.
	 *
	 * @param context the current {@link Context}
	 * @return {@link Disposable} to be disposed after the scheduled {@link Runnable}
	 */
	default Disposable onContextPassing(Context context) {
		return Disposables.disposed();
	}
}
