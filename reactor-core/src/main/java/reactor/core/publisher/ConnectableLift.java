/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 */
final class ConnectableLift<I, O> extends InternalConnectableFluxOperator<I, O> implements Scannable {

	final BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>>
			lifter;

	ConnectableLift(ConnectableFlux<I> p,
			BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
		super(Objects.requireNonNull(p, "source"));
		this.lifter = lifter;
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		this.source.connect(cancelSupport);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return source.getPrefetch();
		if (key == Attr.PARENT) return source;
		if (key == Attr.RUN_STYLE) return Scannable.from(source).scanUnsafe(key);
		return null;
	}

	@Override
	public String stepName() {
		if (source instanceof Scannable) {
			return Scannable.from(source).stepName();
		}
		return super.stepName();
	}

	@Override
	public final CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super O> actual) {
		CoreSubscriber<? super I> input =
				lifter.apply(source, actual);

		Objects.requireNonNull(input, "Lifted subscriber MUST NOT be null");

		return input;
	}
}
