/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class FuseableTest {

	@Test
	void fusionModeName_sync() {
		int mode = Fuseable.SYNC;
		assertThat(Fuseable.fusionModeName(mode)).isEqualTo("SYNC");
		assertThat(Fuseable.fusionModeName(mode, true)).isEqualTo("SYNC");
	}

	@Test
	void fusionModeName_async() {
		int mode = Fuseable.ASYNC;
		assertThat(Fuseable.fusionModeName(mode)).isEqualTo("ASYNC");
		assertThat(Fuseable.fusionModeName(mode, true)).isEqualTo("ASYNC");
	}

	@Test
	void fusionModeName_syncVariants() {
		int mode = Fuseable.SYNC | Fuseable.THREAD_BARRIER;
		assertThat(Fuseable.fusionModeName(mode)).isEqualTo("SYNC+THREAD_BARRIER");
		assertThat(Fuseable.fusionModeName(mode, true)).isEqualTo("SYNC");
	}

	@Test
	void fusionModeName_asyncVariants() {
		int mode = Fuseable.ASYNC | Fuseable.THREAD_BARRIER;
		assertThat(Fuseable.fusionModeName(mode)).isEqualTo("ASYNC+THREAD_BARRIER");
		assertThat(Fuseable.fusionModeName(mode, true)).isEqualTo("ASYNC");
	}

	@Test
	void fusionModeName_unknownThreadBarrier() {
		int mode = 10 | Fuseable.THREAD_BARRIER;
		assertThat(Fuseable.fusionModeName(mode)).isEqualTo("Unknown(10)+THREAD_BARRIER");
		assertThat(Fuseable.fusionModeName(mode, true)).isEqualTo("Unknown(10)");
	}

	@Test
	void fusionModeName_unknownNegative() {
		int mode = -2;
		assertThat(Fuseable.fusionModeName(mode)).isEqualTo("Unknown(-2)");
		assertThat(Fuseable.fusionModeName(mode, true)).isEqualTo("Unknown(-2)");
	}

	@Test
	void fusionModeName_disabled() {
		int mode = -1;
		assertThat(Fuseable.fusionModeName(mode)).isEqualTo("Disabled");
		assertThat(Fuseable.fusionModeName(mode, true)).isEqualTo("Disabled");
	}

	@Test
	void fusionModeName_noneVariants() {
		int mode = Fuseable.NONE | Fuseable.THREAD_BARRIER;
		assertThat(Fuseable.fusionModeName(mode)).isEqualTo("NONE+THREAD_BARRIER");
		assertThat(Fuseable.fusionModeName(mode, true)).isEqualTo("NONE");
	}

}