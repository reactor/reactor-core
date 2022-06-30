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

/**
 * {@code ContextAccessor} to enable reading values from a Reactor
 * {@link ContextView} and writing values to {@link Context}.
 *
 * @author Rossen Stoyanchev
 * @since 3.5.0
 */
public final class ReactorContextAccessor implements ContextAccessor<ContextView, Context> {

	@Override
	public boolean canReadFrom(Class<?> contextType) {
		return ContextView.class.isAssignableFrom(contextType);
	}

	@Override
	public void readValues(ContextView source, Predicate<Object> keyPredicate, Map<Object, Object> target) {
		source.stream()
			.filter(entry -> keyPredicate.test(entry.getKey()))
			.forEach(entry -> target.put(entry.getKey(), entry.getValue()));
	}

	@Override
	public boolean canWriteTo(Class<?> contextType) {
		return Context.class.isAssignableFrom(contextType);
	}

	@Override
	public Context writeValues(Map<Object, Object> source, Context target) {
		return target.putAll(Context.of(source).readOnly());
	}

}
