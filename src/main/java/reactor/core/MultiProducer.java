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

package reactor.core;

import java.util.Iterator;

/**
 * A component that will emit events to N downstreams.
 * @deprecated This internal introspection interface has been removed in favor of
 * centralized, attribute-based {@link Scannable}.
 */
@Deprecated
public interface MultiProducer {

	/**
	 * the connected data receivers
	 * @return the connected data receivers
	 */
	Iterator<?> downstreams();

	/**
	 * the number of downstream receivers
	 * @return the number of downstream receivers
	 */
	default long downstreamCount() {
		return -1L;
	}

	/**
	 * Has any Subscriber attached to this multi-producer ?
	 * @return Has any Subscriber attached to this multi-producer ?
	 */
	default boolean hasDownstreams() {
		return downstreamCount() > 0L;
	}

}
