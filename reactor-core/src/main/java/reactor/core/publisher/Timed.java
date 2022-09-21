/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * @author Simon Baslé
 */
public interface Timed<T> extends Supplier<T> {

	/**
	 * Get the value wrapped by this {@link Timed}.
	 *
	 * @return the onNext value that was timed
	 */
	@Override
	T get();

	/**
	 * Get the elapsed {@link Duration} since the previous onNext (or onSubscribe in
	 * case this represents the first onNext). If possible, nanosecond resolution is used.
	 *
	 * @return the elapsed {@link Duration} since the previous onNext
	 */
	Duration elapsed();

	/**
	 * Get the elapsed {@link Duration} since the subscription (onSubscribe signal).
	 * If possible, nanosecond resolution is used.
	 *
	 * @return the elapsed {@link Duration} since subscription
	 */
	Duration elapsedSinceSubscription();

	/**
	 * Get the timestamp of the emission of this timed onNext, as an {@link Instant}.
	 * It has the same resolution as {@link Instant#ofEpochMilli(long)}.
	 *
	 * @return the epoch timestamp {@link Instant} for this timed onNext
	 */
	Instant timestamp();

}
