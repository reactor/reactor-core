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


import reactor.util.annotation.NonNull;

/**
 * Represents a sequence of events which has an associated key.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public abstract class GroupedFlux<K, V> extends Flux<V> {

	/**
	 * Return the key of the {@link GroupedFlux}.
	 * @return the key
	 */
	@NonNull
	public abstract K key();
}
