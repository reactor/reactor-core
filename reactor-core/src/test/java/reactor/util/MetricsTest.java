/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
public class MetricsTest {

	@Test
	public void smokeTestMicrometerActiveInTests() {
		assertThat(Metrics.isInstrumentationAvailable()).isTrue();
	}

	@Test
	public void setUnsafeRegistryAndGetRegistry() {
		SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();

		assertThat(Metrics.setUnsafeRegistry("foo")).as("string candidate").isFalse();
		assertThat(Metrics.setUnsafeRegistry(simpleMeterRegistry)).as("registry candidate").isTrue();

		assertThat(Metrics.getUnsafeRegistry()).as("get before clear")
		                                       .isSameAs(simpleMeterRegistry);

		assertThat(Metrics.setUnsafeRegistry(null)).as("null candidate")
		                                           .isTrue();
		assertThat(Metrics.getUnsafeRegistry()).as("null candidate cleared")
		                                       .isSameAs(io.micrometer.core.instrument.Metrics.globalRegistry);
	}

}