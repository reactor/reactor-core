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

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class MonoMaterialize<T> extends InternalMonoOperator<T, Signal<T>> {

	MonoMaterialize(Mono<T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Signal<T>> actual) {
		return new MaterializeSubscriber<>(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class MaterializeSubscriber<T> implements InnerOperator<T, Signal<T>> {

		final CoreSubscriber<? super Signal<T>> actual;

		/**
		 * Allows to distinguish onNext+onComplete vs onComplete (ignore the complete in the first case)
		 * while still being able to null out {@link #signalToReplayUponFirstRequest} below.
		 */
		boolean      alreadyReceivedSignalFromSource;
		Subscription s;

		/**
		 * Captures the fact that we were requested. The amount or the number of request
		 * calls doesn't matter since the source is a Mono.
		 */
		volatile boolean   requested;
		/**
		 * This captures an early termination (onComplete or onError), with the goal to
		 * avoid capturing onNext, so as to simplify cleanup and avoid discard concerns.
		 */
		@Nullable
		volatile Signal<T> signalToReplayUponFirstRequest;

		MaterializeSubscriber(CoreSubscriber<? super Signal<T>> actual) {
			this.actual = actual;
		}

		@Override
		public CoreSubscriber<? super Signal<T>> actual() {
			return this.actual;
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (alreadyReceivedSignalFromSource || !requested) {
				//protocol error: there was an onNext, onComplete or onError before (which are all breaking RS or Mono contract)
				Operators.onNextDropped(t, currentContext());
				return;
			}
			alreadyReceivedSignalFromSource = true;
			Signal<T> signal = Signal.next(t, currentContext());
			actual.onNext(signal);
			actual.onComplete();
		}

		@Override
		public void onError(Throwable throwable) {
			if (alreadyReceivedSignalFromSource) {
				//protocol error: there was an onNext, onComplete or onError before (which are all breaking RS or Mono contract)
				Operators.onErrorDropped(throwable, currentContext());
				return;
			}
			alreadyReceivedSignalFromSource = true;
			signalToReplayUponFirstRequest = Signal.error(throwable, currentContext());
			drain();
		}

		@Override
		public void onComplete() {
			if (alreadyReceivedSignalFromSource) {
				//either protocol error, or there was an `onNext` (in which case we already did the job)
				return;
			}
			alreadyReceivedSignalFromSource = true;
			signalToReplayUponFirstRequest = Signal.complete(currentContext());
			drain();
		}

		boolean drain() {
			final Signal<T> signal = signalToReplayUponFirstRequest;
			if (signal != null && requested) {
				actual.onNext(signal);
				actual.onComplete();
				signalToReplayUponFirstRequest = null;
				return true;
			}
			return false;
		}

		@Override
		public void request(long l) {
			if (!this.requested && Operators.validate(l)) {
				this.requested = true; //ignore further requests
				if (drain()) {
					return; //there was an early completion
				}
				s.request(l);
			}
		}

		@Override
		public void cancel() {
			s.cancel();
		}
	}
}
