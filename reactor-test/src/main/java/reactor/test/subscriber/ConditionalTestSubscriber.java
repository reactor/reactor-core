/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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

/**
 * @author Simon Baslé
 */
public class ConditionalTestSubscriber<T> extends TestSubscriber<T> implements Fuseable.ConditionalSubscriber<T> {

	final Predicate<? super T> tryOnNextPredicate;

	ConditionalTestSubscriber(TestSubscriberSpec options,
	                          Predicate<? super T> tryOnNextPredicate) {
		super(options);
		this.tryOnNextPredicate = tryOnNextPredicate;
	}

	@Override
	public boolean tryOnNext(T t) {
		return false;
	}
}
