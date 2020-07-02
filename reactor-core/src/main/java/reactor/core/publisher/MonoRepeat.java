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

import reactor.core.CoreSubscriber;

/**
 * Repeatedly subscribes to the source and relays its values either
 * indefinitely or a fixed number of times.
 * <p>
 * The times == Long.MAX_VALUE is treated as infinite repeat. Times = 1 mirrors the source
 * (the "original" run) and then repeats it once more. Callers should take care of times =
 * 0 and times = -1 cases, which should map to replaying the source and an empty sequence,
 * respectively.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoRepeat<T> extends FluxFromMonoOperator<T, T> {

	final long times;

	MonoRepeat(Mono<? extends T> source, long times) {
		super(source);
		if (times <= 0L) {
			throw new IllegalArgumentException("times > 0 required");
		}
		this.times = times;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		FluxRepeat.RepeatSubscriber<T> parent =
				new FluxRepeat.RepeatSubscriber<>(source, actual, times + 1);

		actual.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.onComplete();
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}
