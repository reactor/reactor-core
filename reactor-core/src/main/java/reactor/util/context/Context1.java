/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.util.context;

import java.util.Objects;
import javax.annotation.Nullable;

final class Context1 implements Context {

	final Object key;
	final Object value;

	Context1(Object key, Object value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public Context put(Object key, @Nullable Object value) {
		Objects.requireNonNull(key, "key");

		if(this.key.equals(key)){
			if (value == null) {
				return Context.empty();
			}
			return new Context1(key, value);
		}
		if (value == null) {
			return this;
		}

		return new ContextN(this.key, this.value, key, value);
	}

	@Override
	public String toString() {
		return "Context1{" + "key=" + key + ", value=" + value + '}';
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key) {
		return this.key.equals(key) ? (T) this.value : null;
	}
}
