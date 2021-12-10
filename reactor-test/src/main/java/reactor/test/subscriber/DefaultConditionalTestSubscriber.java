/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test.subscriber;

import java.util.function.Predicate;

import reactor.core.Fuseable;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;

/**
 * @author Simon Baslé
 */
final class DefaultConditionalTestSubscriber<T> extends DefaultTestSubscriber<T>
	implements ConditionalTestSubscriber<T> {

	final Predicate<? super T> tryOnNextPredicate;

	DefaultConditionalTestSubscriber(TestSubscriberBuilder options,
									 Predicate<? super T> tryOnNextPredicate) {
		super(options);
		this.tryOnNextPredicate = tryOnNextPredicate;
	}

	@Override
	public boolean tryOnNext(T t) {
		int previousState = markOnNextStart();
		boolean wasTerminated = isMarkedTerminated(previousState);
		boolean wasOnNext = isMarkedOnNext(previousState);
		if (wasTerminated || wasOnNext) {
			//at this point, we know we haven't switched the markedOnNext bit. if it is set, let the other onNext unset it
			this.protocolErrors.add(Signal.next(t));
			return false;
		}

		try {
			if (tryOnNextPredicate.test(t)) {
				this.receivedOnNext.add(t);
				if (cancelled.get()) {
					this.receivedPostCancellation.add(t);
				}
				checkTerminatedAfterOnNext();
				return true;
			}
			else {
				Operators.onDiscard(t, currentContext());
				checkTerminatedAfterOnNext();
				return false;
			}
		}
		catch (Throwable predicateError) {
			markOnNextDone();
			internalCancel();
			onError(predicateError);
			return false; //this is consistent with eg. Flux.filter
		}
	}
}
