/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.assertj.core.presentation.StandardRepresentation;

import reactor.core.Fuseable;

/**
 * Utilities around assertions in core tests, eg. AssertJ configuration of representations.
 *
 * @author Simon Baslé
 */
public class AssertionsUtils {

	private static boolean assertJRepresentationInstalled;

	/**
	 * Ensure the custom Reactor-Core tests {@link StandardRepresentation} is installed.
	 */
	public static void installAssertJTestRepresentation() {
		if (!assertJRepresentationInstalled) { //lenient avoidance of reinstalling
			assertJRepresentationInstalled = true;
			Assertions.useRepresentation(REACTOR_CORE_REPRESENTATION);
		}
	}

	private static final StandardRepresentation REACTOR_CORE_REPRESENTATION = new StandardRepresentation() {
		@Override
		protected String toStringOf(AtomicBoolean atomicBoolean) {
			if (atomicBoolean instanceof MemoryUtils.Tracked) {
				return atomicBoolean.toString();
			}
			return super.toStringOf(atomicBoolean);
		}

		@Override
		protected String smartFormat(Iterable<?> iterable) {
			if (iterable instanceof Fuseable.QueueSubscription) {
				return String.valueOf(iterable);
			}
			return super.smartFormat(iterable);
		}
	};
}
