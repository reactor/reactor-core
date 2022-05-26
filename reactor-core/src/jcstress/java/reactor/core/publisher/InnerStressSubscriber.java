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

public class InnerStressSubscriber<T> extends StressSubscriber<T> {

	final StressSubscriber<?> parent;

	InnerStressSubscriber(StressSubscriber<?> parent) {
		this.parent = parent;
	}

	@Override
	public void onComplete() {
		super.onComplete();
		this.parent.subscription.request(1);
	}

	@Override
	public void cancel() {
		super.cancel();
	}
}