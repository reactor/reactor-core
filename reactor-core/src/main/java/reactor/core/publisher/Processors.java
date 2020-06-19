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

import java.time.Duration;
import java.util.Queue;
import java.util.function.Consumer;

import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * A class that bears factory methods for all flavors of Reactor {@link org.reactivestreams.Processor}.
 * For manually pushing signals, see {@link Sinks} instead.
 *
 * @author Simon Baslé
 */
public final class Processors {

	@SuppressWarnings("deprecation")
	public static <T> FluxIdentityProcessor<T> unicast() {
		return UnicastProcessor.create();
	}

	@SuppressWarnings("deprecation")
	public static <T> FluxIdentityProcessor<T> multicast() {
		return EmitterProcessor.create();
	}

	@SuppressWarnings("deprecation")
	public static <T> FluxIdentityProcessor<T> replayAll() {
		return ReplayProcessor.create();
	}

	@SuppressWarnings("deprecation")
	public static <T> FluxIdentityProcessor<T> replay(int historySize) {
		return ReplayProcessor.create(historySize);
	}

	@SuppressWarnings("deprecation")
	public static  <T> FluxIdentityProcessor<T> replayTimeout(Duration maxAge) {
		return ReplayProcessor.createTimeout(maxAge);
	}

	@SuppressWarnings("deprecation")
	public static <T> FluxIdentityProcessor<T> replaySizeAndTimeout(int historySize, Duration maxAge) {
		return ReplayProcessor.createSizeAndTimeout(historySize, maxAge);
	}

	public static MoreProcessors more() {
		return MoreProcessors.INSTANCE;
	}

	public static final class MoreProcessors {

		static final MoreProcessors INSTANCE = new MoreProcessors();

		private MoreProcessors() {
		}

		// == unicast ==

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> unicast(Queue<T> queue) {
			return UnicastProcessor.create(queue);
		}

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> unicast(Queue<T> queue, Disposable endCallback) {
			return UnicastProcessor.create(queue, endCallback);
		}

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> unicast(Queue<T> queue, Consumer<? super T> onOverflow, Disposable endCallback) {
			return UnicastProcessor.create(queue, onOverflow, endCallback);
		}

		// == direct (less used) ==

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> multicastNoBackpressure() {
			return DirectProcessor.create();
		}

		// == emitter ==
		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> multicast(boolean autoCancel) {
			return EmitterProcessor.create(autoCancel);
		}

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> multicast(int bufferSize) {
			return EmitterProcessor.create(bufferSize);
		}

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> multicast(int bufferSize, boolean autoCancel) {
			return EmitterProcessor.create(bufferSize, autoCancel);
		}

		// == replay ==

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> replayLatest() {
			return ReplayProcessor.cacheLast();
		}

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> replayLatestOrDefault(@Nullable T value) {
			return ReplayProcessor.cacheLastOrDefault(value);
		}

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> replay(int historySize, boolean unbounded) {
			return ReplayProcessor.create(historySize, unbounded);
		}

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> replayTimeout(Duration maxAge, Scheduler scheduler) {
			return ReplayProcessor.createTimeout(maxAge, scheduler);
		}

		@SuppressWarnings("deprecation")
		public final <T> FluxIdentityProcessor<T> replaySizeAndTimeout(int historySize, Duration maxAge, Scheduler scheduler) {
			return ReplayProcessor.createSizeAndTimeout(historySize, maxAge, scheduler);
		}

	}

}
