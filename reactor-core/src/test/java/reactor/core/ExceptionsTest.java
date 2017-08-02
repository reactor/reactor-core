/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.junit.Test;
import reactor.test.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
public class ExceptionsTest {

	volatile Throwable addThrowable;
	static final AtomicReferenceFieldUpdater<ExceptionsTest, Throwable> ADD_THROWABLE =
		AtomicReferenceFieldUpdater.newUpdater(ExceptionsTest.class, Throwable.class, "addThrowable");

	@Test
	public void addThrowableRace() throws Exception {
		for (int i = 0; i < 10; i++) {
			final int idx = i;
			RaceTestUtils.race(
					() -> Exceptions.addThrowable(ADD_THROWABLE, ExceptionsTest.this, new IllegalStateException("boomState" + idx)),
					() -> Exceptions.addThrowable(ADD_THROWABLE, ExceptionsTest.this, new IllegalArgumentException("boomArg" + idx))
			);
		}

		addThrowable.printStackTrace();

		assertThat(addThrowable.getSuppressed())
				.hasSize(20);
	}

}