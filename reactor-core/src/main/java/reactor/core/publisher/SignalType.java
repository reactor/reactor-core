/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

/**
 * Reactive Stream signal types
 */
public enum SignalType {

	/**
	 * A signal when the subscription is triggered
	 */
	SUBSCRIBE,
	/**
	 * A signal when a request is made through the subscription
	 */
	REQUEST,
	/**
	 * A signal when the subscription is cancelled
	 */
	CANCEL,
	/**
	 * A signal when an operator receives a subscription
	 */
	ON_SUBSCRIBE,
	/**
	 * A signal when an operator receives an emitted value
	 */
	ON_NEXT,
	/**
	 * A signal when an operator receives an error
	 */
	ON_ERROR,
	/**
	 * A signal when an operator completes
	 */
	ON_COMPLETE,
	/**
	 * A signal that can be propagated AFTER an operator has terminated
	 */
	AFTER_TERMINATE;

	@Override
	public String toString() {
		switch (this) {
			case ON_SUBSCRIBE:
				return "onSubscribe";
			case ON_NEXT:
				return "onNext";
			case ON_ERROR:
				return "onError";
			case ON_COMPLETE:
				return "onComplete";
			case REQUEST:
				return "request";
			case CANCEL:
				return "cancel";
			case AFTER_TERMINATE:
				return "afterTerminate";
			default:
				return "subscribe";
		}
	}
}
