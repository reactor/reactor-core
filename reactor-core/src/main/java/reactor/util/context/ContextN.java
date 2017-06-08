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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

@SuppressWarnings("unchecked")
final class ContextN extends HashMap<Object, Object>
		implements Context {

	ContextN(Object key1, Object value1, Object key2, Object value2) {
		super(2, 1f);
		super.put(key1, value1);
		super.put(key2, value2);
	}

	ContextN(Map<Object, Object> map, Object key, @Nullable Object value) {
		super(map.size() + 1, 1f);
		putAll(map);
		super.put(key, value);
	}

	@Override
	public Context put(Object key, @Nullable Object value) {
		Objects.requireNonNull(key, "key");
		return new ContextN(this, key, value);
	}

	@Override
	@Nullable
	public Object get(Object key) {
		return super.get(key);
	}

	@Override
	@Nullable
	public Object getOrDefault(Object key, @Nullable Object defaultValue) {
		Object v = get(key);
		if(v == null){
			return defaultValue;
		}
		return v;
	}
}
