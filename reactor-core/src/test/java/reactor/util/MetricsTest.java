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
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
public class MetricsTest {

	@After
	public void resetRegistry() {
		Metrics.defaultRegistry = null;
	}

	@Test
	public void smokeTestMicrometerActiveInTests() {
		assertThat(Metrics.isMicrometerAvailable()).isTrue();
	}

	@Test
	public void setAndGetMeterRegistry() {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		Metrics.setRegistryCandidate(registry);

		assertThat(Metrics.getRegistryCandidate()).isSameAs(registry);
	}

	@Test
	public void setMeterRegistryNullResets() {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		Metrics.setRegistryCandidate(registry);

		assertThat(Metrics.getRegistryCandidate()).isSameAs(registry);

		Metrics.setRegistryCandidate(null);

		assertThat(Metrics.getRegistryCandidate()).isNull();
	}

	@Test
	public void setMeterRegistryNullClosesOld() {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		Metrics.setRegistryCandidate(registry);

		assertThat(Metrics.getRegistryCandidate()).isSameAs(registry);

		Metrics.setRegistryCandidate(null);

		assertThat(registry.isClosed()).as("old registry closed").isTrue();
	}

	@Test
	public void replaceMeterRegistryClosesOld() {
		SimpleMeterRegistry oldRegistry = new SimpleMeterRegistry();
		SimpleMeterRegistry newRegistry = new SimpleMeterRegistry();

		Metrics.setRegistryCandidate(oldRegistry);

		assertThat(oldRegistry.isClosed()).as("open when set").isFalse();

		Metrics.setRegistryCandidate(newRegistry);
		assertThat(oldRegistry.isClosed()).as("closed when replaced").isTrue();
	}
}