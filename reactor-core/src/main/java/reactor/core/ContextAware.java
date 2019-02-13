/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor.core;

import reactor.util.context.Context;

/**
 * A common interface for the {@link Context} aware abstractions.
 *
 * @see reactor.core.scheduler.Scheduler.ContextRunnable
 * @see CoreSubscriber
 *
 * @author Sergei Egorov
 *
 * @since 3.3.0
 */
public interface ContextAware {

	/**
	 * Request a {@link Context} from dependent components which can include downstream
	 * operators during subscribing or a terminal {@link org.reactivestreams.Subscriber}.
	 *
	 * @return a resolved context or {@link Context#empty()}
	 */
	Context currentContext();
}
