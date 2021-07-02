/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
 * Strategies to deal with overflow of a buffer during
 * {@link Flux#onBackpressureBuffer(int, BufferOverflowStrategy)
 * backpressure buffering}.
 *
 * @author Simon Baslé
 */
public enum BufferOverflowStrategy {

	/**
	 * Propagate an {@link IllegalStateException} when the buffer is full.
	 */
	ERROR,
	/**
	 * Drop the new element without propagating an error when the buffer is full.
	 */
	DROP_LATEST,
	/**
	 * When the buffer is full, remove the oldest element from it and offer the
	 * new element at the end instead. Do not propagate an error.
	 */
	DROP_OLDEST

}
