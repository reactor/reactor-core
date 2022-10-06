/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util.context;

import java.util.Map;
import java.util.function.Predicate;

import io.micrometer.context.ContextAccessor;

import reactor.util.annotation.Nullable;

/**
 * A {@code ContextAccessor} to enable reading values from a Reactor
 * {@link ContextView} and writing values to {@link Context}.
 * <p>
 * Please note that this public class implements the {@code libs.micrometer.contextPropagation}
 * SPI library, which is an optional dependency.
 *
 * @author Rossen Stoyanchev
 * @author Simon Baslé
 * @since 3.5.0
 */
public final class ReactorContextAccessor implements ContextAccessor<ContextView, Context> {

	@Override
	public Class<? extends ContextView> readableType() {
		return ContextView.class;
	}

	@Override
	public void readValues(ContextView source, Predicate<Object> keyPredicate, Map<Object, Object> target) {
		source.forEach((k, v) -> {
			if (keyPredicate.test(k)) {
				target.put(k, v);
			}
		});
	}

	@Override
	@Nullable
	public <T> T readValue(ContextView sourceContext, Object key) {
		return sourceContext.getOrDefault(key, null);
	}

	@Override
	public Class<? extends Context> writeableType() {
		return Context.class;
	}

	@Override
	public Context writeValues(Map<Object, Object> source, Context target) {
		return target.putAllMap(source);
	}
}
