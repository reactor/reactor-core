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

/**
 * Abstract base to optimize interactions between reactor core {@link Context} implementations.
 *
 * @author Simon Baslé
 */
interface CoreContext extends Context {

	@Override
	default boolean isEmpty() {
		// Overridden in Context0#isEmpty
		return false;
	}
	
	@Override
	default Context putAll(Context other) {
		if (other.isEmpty()) return this;

		if (other instanceof CoreContext) {
			CoreContext coreContext = (CoreContext) other;
			return coreContext.putAllInto(this);
		}

		ContextN newContext = new ContextN(this.size() + other.size());
		this.unsafePutAllInto(newContext);
		other.stream().sequential().forEach(newContext);
		if (newContext.size() <= 5) {
			// make it return Context{1-5}
			return Context.of(newContext);
		}
		return newContext;
	}

	/**
	 * Let this Context add its internal values to the given base Context, avoiding creating
	 * intermediate holders for key-value pairs as much as possible.
	 *
	 * @param base the {@link Context} in which we're putting all our values
	 * @return a new context containing all the base values merged with all our values
	 */
	Context putAllInto(Context base);

	/**
	 * Let this Context add its internal values to the given ContextN, <strong>mutating it</strong>,
	 * but avoiding creating intermediate holders for key-value pairs as much as possible.
	 *
	 * @param other the {@link ContextN} we're mutating by putting all our values into
	 */
	void unsafePutAllInto(ContextN other);

}
