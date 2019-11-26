/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.context;

import java.util.Map;

/**
 * Abstract base to optimize interactions between reactor core {@link Context} implementations.
 *
 * @author Simon Baslé
 */
abstract class AbstractContext implements Context {

	@Override
	public boolean isEmpty() {
		// Overridden in Context0#isEmpty
		return false;
	}

	/**
	 * Let this Context add its internal values to the given base Context, avoiding creating
	 * intermediate holders for key-value pairs as much as possible.
	 *
	 * @param base the {@link Context} in which we're putting all our values
	 * @return a new context containing all the base values merged with all our values
	 */
	protected abstract Context putAllInto(Context base);

	/**
	 * Let this Context add its internal values to the given Map, avoiding creating
	 * intermediate holders for key-value pairs as much as possible.
	 *
	 * @param map the {@link Map} in which we're putting all our values
	 */
	protected abstract void putAllInto(Map<Object, Object> map);

}
