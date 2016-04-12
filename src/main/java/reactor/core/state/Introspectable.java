/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.state;

public interface Introspectable {

	/**
	 * A component that is meant to be introspectable on finest logging level
	 */
	int TRACE_ONLY = 0b00000001;

	/**
	 * A component that is meant to be embedded or gating linked upstream(s) and/or downstream(s) components
	 */
	int INNER = 0b00000010;

	/**
	 * A component that is intended to build others
	 */
	int FACTORY = 0b00000100;

	/**
	 * An identifiable component
	 */
	int UNIQUE = 0b0001000;

	/**
	 * A component that emits traces with the following standard :
	 * <pre>message, arg1: signalType, arg2: signalPayload and arg3: this</pre>
	 */
	int LOGGING = 0b000010000;

	/**
	 * Flags determining the nature of this {@link Introspectable}, can be a combination of those, e.g. :
	 * <pre>
	 *     int mode = Introspectable.LOGGING | Introspectable.FACTORY
	 *
	 * @return the current reactive modes
	 */
	default int getMode() {
		return 0;
	}


	/**
	 * The name of the component
	 * @return the name of the component
	 */
	default String getName() {
		return getClass().getSimpleName();
	}


	/**
	 * Defined identifier or null if not available
	 * @return defined identifier or null if not available
	 */
	default Object key() {
		return null;
	}

	/**
	 * Current error if any, default to null
	 * @return Current error if any, default to null
	 */
	default Throwable getError(){
		return null;
	}

	/**
	 * Can represent a period duration if the component is timed otherwise -1L
	 * @return a period duration if the component is timed or -1L
	 */
	default long getPeriod(){
		return -1L;
	}
}
