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

import java.util.function.Consumer;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Hide a {@link ConnectableFlux} from fusion optimizations while keeping the {@link ConnectableFlux}
 * specific API visible.
 *
 * @author Simon Baslé
 */
final class ConnectableFluxHide<T> extends InternalConnectableFluxOperator<T, T> implements Scannable {

	ConnectableFluxHide(ConnectableFlux<T> source) {
		super(source);
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public final CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return actual;
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		source.connect(cancelSupport);
	}
}
