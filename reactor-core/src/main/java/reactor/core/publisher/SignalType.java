/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
	SUBSCRIBE("subscribe"),
	/**
	 * A signal when a request is made through the subscription
	 */
	REQUEST("request"),
	/**
	 * A signal when the subscription is cancelled
	 */
	CANCEL("cancel"),
	/**
	 * A signal when an operator receives a subscription
	 */
	ON_SUBSCRIBE("onSubscribe"),
	/**
	 * A signal when an operator receives an emitted value
	 */
	ON_NEXT("onNext"),
	/**
	 * A signal when an operator receives an error
	 */
	ON_ERROR("onError"),
	/**
	 * A signal when an operator completes
	 */
	ON_COMPLETE("onComplete"),
	/**
	 * A signal when an operator completes
	 */
	AFTER_TERMINATE("afterTerminate"),
	/**
	 * A context read signal
	 */
	CURRENT_CONTEXT("currentContext"),
	/**
	 * A context update signal
	 */
	ON_CONTEXT_UPDATE("onContextUpdate");

	private String name;

	SignalType(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return name;
	}
}
