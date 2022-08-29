/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Predicate;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * See {@link FluxOnErrorReturn}.
 *
 * @author Simon Baslé
 */
final class MonoOnErrorReturn<T> extends InternalMonoOperator<T, T> {

	@Nullable
	final Predicate<? super Throwable> resumableErrorPredicate;

	@Nullable
	final T fallbackValue;

	MonoOnErrorReturn(Mono<? extends T> source, @Nullable Predicate<? super Throwable> predicate, @Nullable T value) {
		super(source);
		resumableErrorPredicate = predicate;
		fallbackValue = value;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new FluxOnErrorReturn.ReturnSubscriber<>(actual, resumableErrorPredicate, fallbackValue, false);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}
