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

package reactor.core.publisher;

import java.util.function.Predicate;

import org.reactivestreams.Subscription;
import reactor.core.Fuseable;

public class ConditionalStressSubscriber<T> extends StressSubscriber<T> implements Fuseable.ConditionalSubscriber<T> {


	final Predicate<T> tryOnNextPredicate;

	/**
	 * Build a {@link ConditionalStressSubscriber} that makes an unbounded request upon subscription.
	 */
	public ConditionalStressSubscriber() {
		this(Long.MAX_VALUE, (__) -> true);
	}

	/**
	 * Build a {@link ConditionalStressSubscriber} that that makes an unbounded request upon
	 * subscription.
	 *
	 * @param tryOnNextPredicate the tryOnNext predicate
	 */
	public ConditionalStressSubscriber(Predicate<T> tryOnNextPredicate) {
		this(Long.MAX_VALUE, tryOnNextPredicate);
	}

	/**
	 * Build a {@link ConditionalStressSubscriber} that requests the provided amount in
	 * {@link #onSubscribe(Subscription)}. Use {@code 0} to avoid any initial request
	 * upon subscription.
	 *
	 * @param initRequest the requested amount upon subscription, or zero to disable initial request
	 */
	public ConditionalStressSubscriber(long initRequest) {
		this(initRequest, __ -> true);
	}

	/**
	 * Build a {@link ConditionalStressSubscriber} that requests the provided amount in
	 * {@link #onSubscribe(Subscription)}. Use {@code 0} to avoid any initial request
	 * upon subscription.
	 *
	 * @param initRequest the requested amount upon subscription, or zero to disable initial request
	 * @param tryOnNextPredicate the tryOnNext predicate
	 */
	public ConditionalStressSubscriber(long initRequest, Predicate<T> tryOnNextPredicate) {
		super(initRequest);
		this.tryOnNextPredicate = tryOnNextPredicate;
	}

	@Override
	public boolean tryOnNext(T value) {
		onNext(value);
		return tryOnNextPredicate.test(value);
	}
}
