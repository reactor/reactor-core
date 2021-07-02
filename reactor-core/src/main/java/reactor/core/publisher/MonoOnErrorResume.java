/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * Resumes the failed main sequence with another sequence returned by
 * a function for the particular failure exception.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoOnErrorResume<T> extends InternalMonoOperator<T, T> {

	final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

	MonoOnErrorResume(Mono<? extends T> source,
						   Function<? super Throwable, ? extends Mono<? extends T>>
								   nextFactory) {
		super(source);
		this.nextFactory = Objects.requireNonNull(nextFactory, "nextFactory");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new FluxOnErrorResume.ResumeSubscriber<>(actual, nextFactory);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}
