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

/**
 * A component that is forking to a sub-flow given a delegate input and that is consuming from a given delegate output
 * @deprecated This internal introspection interface has been removed in favor of
 * centralized, attribute-based {@link Scannable}.
 */
@Deprecated
public interface Loopback {

	/**
	 * @return component delegated to for incoming data or {@code null} if unavailable
	 */
	default Object connectedInput() {
		return null;
	}

	/**
	 * @return component delegated to for outgoing data or {@code null} if unavailable
	 */
	default Object connectedOutput() {
		return null;
	}
}
