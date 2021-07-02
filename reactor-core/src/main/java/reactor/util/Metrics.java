/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util;

import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Flux;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

/**
 * Utilities around instrumentation and metrics with Micrometer.
 *
 * @author Simon Baslé
 */
public class Metrics {

	static final boolean isMicrometerAvailable;

	static {
		boolean micrometer;
		try {
			globalRegistry.getRegistries();
			micrometer = true;
		}
		catch (Throwable t) {
			micrometer = false;
		}
		isMicrometerAvailable = micrometer;
	}

	/**
	 * Check if the current runtime supports metrics / instrumentation, by
	 * verifying if Micrometer is on the classpath.
	 *
	 * @return true if the Micrometer instrumentation facade is available
	 */
	public static final boolean isInstrumentationAvailable() {
		return isMicrometerAvailable;
	}

	public static class MicrometerConfiguration {

		private static MeterRegistry registry = globalRegistry;

		/**
		 * Set the registry to use in reactor for metrics related purposes.
		 * @return the previously configured registry.
		 */
		public static MeterRegistry useRegistry(MeterRegistry registry) {
			MeterRegistry previous = MicrometerConfiguration.registry;
			MicrometerConfiguration.registry = registry;
			return previous;
		}

		/**
		 * Get the registry used in reactor for metrics related purposes.
		 * @see Flux#metrics()
		 */
		public static MeterRegistry getRegistry() {
			return MicrometerConfiguration.registry;
		}
	}

}
