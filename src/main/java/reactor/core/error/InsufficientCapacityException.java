/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.core.error;

import reactor.core.support.ReactiveState;

/**
 * <p>Exception thrown when the it is not possible to dispatch a signal
 * due to insufficient capacity.
 *
 * @author Stephane Maldini
 */
@SuppressWarnings("serial")
public final class InsufficientCapacityException extends RuntimeException {
	private static final InsufficientCapacityException INSTANCE = new InsufficientCapacityException();

	private InsufficientCapacityException() {
		super("The subscriber is overrun by more signals than expected (bounded queue...)");
	}

	public static InsufficientCapacityException get() {
		return ReactiveState.TRACE_NOCAPACITY ? new InsufficientCapacityException() : INSTANCE;
	}

	@Override
	public synchronized Throwable fillInStackTrace() {
		return ReactiveState.TRACE_NOCAPACITY ? super.fillInStackTrace() : this;
	}
}
