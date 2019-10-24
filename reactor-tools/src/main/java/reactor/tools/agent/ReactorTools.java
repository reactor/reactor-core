/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor.tools.agent;

import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.util.function.Function;

import net.bytebuddy.agent.ByteBuddyAgent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactorTools {

	private static Boolean DETECT_CONTEXT_LOSS = null;

	/**
	 * Globally enables the {@link reactor.util.context.Context} loss detection that may happen
	 * when you use operators like {@link reactor.core.publisher.Mono#transform(Function)}
	 * or {@link reactor.core.publisher.Flux#transformDeferred(Function)} with non-Reactor types.
	 *
	 * An exception will be thrown if after applying the transformations a new {@link reactor.util.context.Context}
	 * was returned (or no context at all) instead of changing the current one.
	 */
	public static synchronized void enableContextLossTracking() {
		if (DETECT_CONTEXT_LOSS == null) {
			Instrumentation instrumentation = ByteBuddyAgent.install();

			ContextLossDetectionTransformer transformer = new ContextLossDetectionTransformer();
			instrumentation.addTransformer(transformer, true);

			try {
				// Retransform in case the classes were loaded already
				instrumentation.retransformClasses(Mono.class, Flux.class);
			}
			catch (UnmodifiableClassException e) {
				throw new RuntimeException(e);
			}
		}
		DETECT_CONTEXT_LOSS = true;
	}

	/**
	 * Globally disables the {@link reactor.util.context.Context} loss detection that was previously
	 * enabled by {@link #enableContextLossTracking()}.
	 *
	 */
	public static synchronized void disableContextLossTracking() {
		if (DETECT_CONTEXT_LOSS == null) {
			return;
		}
		DETECT_CONTEXT_LOSS = false;
	}

	private ReactorTools() {

	}
}
