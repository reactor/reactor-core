/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.subscription;

import org.reactivestreams.Subscription;
import reactor.core.support.ReactiveState;

/**
 * A singleton Subscription that represents a cancelled subscription instance and should not be leaked to clients as it
 * represents a terminal state. <br> If algorithms need to hand out a subscription, replace this with {@link
 * EmptySubscription#INSTANCE} because there is no standard way to tell if a Subscription is cancelled or not
 * otherwise.
 */
public enum CancelledSubscription implements Subscription, ReactiveState.ActiveDownstream {
	INSTANCE;

	@Override
	public boolean isCancelled() {
		return true;
	}

	@Override
	public void request(long n) {
		// deliberately no op
	}

	@Override
	public void cancel() {
		// deliberately no op
	}

}
