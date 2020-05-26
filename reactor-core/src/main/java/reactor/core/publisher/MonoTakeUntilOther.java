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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class MonoTakeUntilOther<T, U> extends InternalMonoOperator<T, T> {

	private final Publisher<U> other;

	MonoTakeUntilOther(Mono<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		FluxTakeUntilOther.TakeUntilMainSubscriber<T> mainSubscriber = new FluxTakeUntilOther.TakeUntilMainSubscriber<>(
				actual);

		FluxTakeUntilOther.TakeUntilOtherSubscriber<U> otherSubscriber = new FluxTakeUntilOther.TakeUntilOtherSubscriber<>(mainSubscriber);

		other.subscribe(otherSubscriber);
		return mainSubscriber;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}
