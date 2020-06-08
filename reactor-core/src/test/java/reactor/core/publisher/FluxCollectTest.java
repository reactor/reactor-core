/*
 * Copyright (c) 2011-2020 Pivotal Software Inc, All Rights Reserved.
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


package reactor.core.publisher;

import java.util.stream.Collector;

import org.junit.Test;
import reactor.test.StepVerifier;

import static java.util.stream.Collectors.reducing;

/**
 * Tests for {@link Flux#collect(Collector)}.
 *
 * @author Eric Bottard
 */
public class FluxCollectTest {

	/**
	 * A collector producing null should be intercepted early instead of signaling onNext(null).
	 * @see <a href="https://github.com/reactor/reactor-core/issues/2181" target="_top">issue 2181</a>
	 */
	@Test
	public void collectHandlesNulls() {
		StepVerifier.create(Flux.empty().collect(reducing(null, (a, b) -> a)))
				.verifyError(NullPointerException.class);
	}
}
