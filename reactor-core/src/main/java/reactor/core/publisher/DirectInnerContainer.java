/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

/**
 * A package-private interface allowing to mutualize logic between {@link DirectProcessor}
 * and {@link SinkManyBestEffort}.
 *
 * @author Simon Baslé
 * @deprecated remove again once DirectProcessor is removed
 */
@Deprecated
interface DirectInnerContainer<T> {

	/**
	 * Add a new {@link SinkManyBestEffort.DirectInner} to this publisher.
	 *
	 * @param s the new {@link SinkManyBestEffort.DirectInner} to add
	 *
	 * @return {@code true} if the inner could be added, {@code false} if the publisher cannot accept new subscribers
	 */
	boolean add(SinkManyBestEffort.DirectInner<T> s);

	/**
	 * Remove an {@link SinkManyBestEffort.DirectInner} from this publisher. Does nothing if the inner is not currently managed
	 * by the publisher.
	 *
	 * @param s the  {@link SinkManyBestEffort.DirectInner} to remove
	 */
	void remove(SinkManyBestEffort.DirectInner<T> s);
}
