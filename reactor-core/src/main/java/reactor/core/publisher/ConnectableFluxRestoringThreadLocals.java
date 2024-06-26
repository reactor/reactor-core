/*
 * Copyright (c) 2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.function.Consumer;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;

class ConnectableFluxRestoringThreadLocals<T> extends ConnectableFlux<T> {

	private final ConnectableFlux<T> source;

	public ConnectableFluxRestoringThreadLocals(ConnectableFlux<T> source) {
		this.source = Objects.requireNonNull(source, "source");
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		source.connect(cancelSupport);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(Operators.restoreContextOnSubscriber(source, actual));
	}
}
