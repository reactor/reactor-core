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

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;

final class Context0 implements Context {

	static final Context0 INSTANCE = new Context0();

	@Override
	public Context put(Object key, @Nullable Object value) {
		Objects.requireNonNull(key, "key");
		if(value == null){
			return this;
		}
		return new Context1(key, value);
	}

	@Override
	@Nullable
	public <T> T get(Object key) {
		return null;
	}

	@Override
	public String toString() {
		return "Context0{}";
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		return Stream.empty();
	}
}
