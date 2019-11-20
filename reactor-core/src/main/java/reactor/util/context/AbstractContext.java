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
 * @author Simon Baslé
 */
abstract class AbstractContext implements Context {

	@Override
	public Context putAll(Context other) {
		if (other.isEmpty()) return this;
		if (other instanceof AbstractContext) {
			return ((AbstractContext) other).putAllSelfInto(this);
		}
		return Context.super.putAll(other);
	}

	@Override
	public abstract AbstractContext put(Object key, Object value);

	protected abstract AbstractContext putAllSelfInto(AbstractContext initial);

}
