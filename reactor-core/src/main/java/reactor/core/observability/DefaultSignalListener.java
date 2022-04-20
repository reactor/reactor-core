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

package reactor.core.observability;

import reactor.core.Fuseable;
import reactor.core.publisher.SignalType;

/**
 * A default implementation of a {@link SignalListener} with all the handlers no-op.
 *
 * @author Simon Baslé
 */
public abstract class DefaultSignalListener<T> implements SignalListener<T> {

	/**
	 * This listener actually captures the negotiated fusion
	 * mode (if any) and exposes this information to child classes via #getFusionMode().
	 */
	int fusionMode = Fuseable.NONE;

	@Override
	public void doFirst() throws Throwable {
	}

	@Override
	public void doFinally(SignalType terminationType) throws Throwable {
	}

	@Override
	public void doOnSubscription() throws Throwable {
	}

	@Override
	public void doOnFusion(int negotiatedFusion) throws Throwable {
		this.fusionMode = negotiatedFusion;
	}

	/**
	 * Return the fusion mode negotiated with the source: {@link Fuseable#SYNC} and {@link Fuseable#ASYNC}) as relevant
	 * if some fusion was negotiated. {@link Fuseable#NONE} if fusion was never requested, or if it couldn't be negotiated.
	 *
	 * @return the negotiated fusion mode, if any
	 */
	protected int getFusionMode() {
		return fusionMode;
	}

	@Override
	public void doOnRequest(long requested) throws Throwable {
	}

	@Override
	public void doOnCancel() throws Throwable {
	}

	@Override
	public void doOnNext(T value) throws Throwable {
	}

	@Override
	public void doOnComplete() throws Throwable {
	}

	@Override
	public void doOnError(Throwable error) throws Throwable {
	}

	@Override
	public void doAfterComplete() throws Throwable {
	}

	@Override
	public void doAfterError(Throwable error) throws Throwable {
	}

	@Override
	public void doOnMalformedOnNext(T value) throws Throwable {
	}

	@Override
	public void doOnMalformedOnComplete() throws Throwable {
	}

	@Override
	public void doOnMalformedOnError(Throwable error) throws Throwable {
	}

	@Override
	public void handleListenerError(Throwable listenerError) {
	}
}
