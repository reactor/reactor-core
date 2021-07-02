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
import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;

/**
 * Repeats a source when a companion sequence signals an item in response to the main's
 * completion signal
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat attempt
 * is suppressed and any terminal signal will terminate the main source with the same
 * signal immediately.
 *
 * @param <T> the source value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoRepeatWhen<T> extends FluxFromMonoOperator<T, T> {

	final Function<? super Flux<Long>, ? extends Publisher<?>> whenSourceFactory;

	MonoRepeatWhen(Mono<? extends T> source,
			Function<? super Flux<Long>, ? extends Publisher<?>> whenSourceFactory) {
		super(source);
		this.whenSourceFactory =
				Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		FluxRepeatWhen.RepeatWhenOtherSubscriber other =
				new FluxRepeatWhen.RepeatWhenOtherSubscriber();

		CoreSubscriber<T> serial = Operators.serialize(actual);

		FluxRepeatWhen.RepeatWhenMainSubscriber<T> main =
				new FluxRepeatWhen.RepeatWhenMainSubscriber<>(serial, other.completionSignal, source);
		other.main = main;

		serial.onSubscribe(main);

		Publisher<?> p;

		try {
			p = Objects.requireNonNull(whenSourceFactory.apply(other),
					"The whenSourceFactory returned a null Publisher");
		}
		catch (Throwable e) {
			actual.onError(Operators.onOperatorError(e, actual.currentContext()));
			return null;
		}

		p.subscribe(other);

		if (!main.cancelled) {
			return main;
		}
		else {
			return null;
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}
