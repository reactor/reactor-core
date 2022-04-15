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

package reactor.test;

import java.util.function.BiFunction;

import io.micrometer.contextpropagation.ContextAccessor;
import io.micrometer.contextpropagation.ContextContainer;
import io.micrometer.contextpropagation.Namespace;
import io.micrometer.contextpropagation.ThreadLocalAccessor;

import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * A micrometer context-propagation-api {@link ThreadLocalAccessor} and {@link ContextAccessor} used to propagate
 * {@link reactor.test.ContextPropagationUtils.ThreadLocalHelper} values in tests around the
 * {@link reactor.core.scheduler.Schedulers#onScheduleContextualHook(String, BiFunction)} hook.
 *
 * @author Simon Baslé
 */
public final class ContextPropagationUtilsPropagator implements ThreadLocalAccessor, ContextAccessor<ContextView, Context> {

	private static final String    NAMESPACE_PREFIX = "reactor.context.propagation.utils";
	private static final Namespace NAMESPACE        = Namespace.of(NAMESPACE_PREFIX);
	/* for test */ static final String IN_CONTEXT_KEY = NAMESPACE_PREFIX + ".tl";

	@Override
	public void captureValues(ContextView view, ContextContainer container) {
		container.put(IN_CONTEXT_KEY, view.getOrDefault(IN_CONTEXT_KEY, "NOT_FOUND"));
	}

	@Override
	public Context restoreValues(Context context, ContextContainer container) {
		String value = container.get(IN_CONTEXT_KEY);
		if (value == null) return context;
		return context.put(IN_CONTEXT_KEY, value);
	}

	@Override
	public Namespace getNamespace() {
		return NAMESPACE;
	}

	@Override
	public void captureValues(ContextContainer container) {
		ThreadLocal<String> tlToUse = ContextPropagationUtils.THREAD_LOCAL_REF.get();
		if (tlToUse != null) {
			container.put(IN_CONTEXT_KEY, tlToUse.get());
		}
	}

	@Override
	public void restoreValues(ContextContainer container) {
		ThreadLocal<String> tlToUse = ContextPropagationUtils.THREAD_LOCAL_REF.get();
		if (tlToUse != null) {
			tlToUse.set(container.get(IN_CONTEXT_KEY));
		}
	}

	@Override
	public void resetValues(ContextContainer container) {
		ThreadLocal<String> tlToUse = ContextPropagationUtils.THREAD_LOCAL_REF.get();
		if (tlToUse != null) {
			tlToUse.remove();
		}
	}

	@Override
	public Class<?> getSupportedContextClassForSet() {
		return Context.class;
	}

	@Override
	public Class<?> getSupportedContextClassForGet() {
		return ContextView.class;
	}
}
