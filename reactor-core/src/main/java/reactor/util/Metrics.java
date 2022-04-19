/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
 * Deprecated as of 3.5.0, prefer using the new reactor-core-micrometer module.
 *
 * @author Simon Baslé
 * @deprecated prefer using the new reactor-core-micrometer module Micrometer entrypoint. To be removed in 3.6.0 at the earliest.
 */
@Deprecated
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
	 * <p>
	 * Note that this is regardless of whether the new reactor-core-micrometer module is also on the classpath (which
	 * could be the reason Micrometer is on the classpath in the first place).
	 *
	 * @return true if the Micrometer instrumentation facade is available
	 * @deprecated prefer explicit usage of the reactor-core-micrometer module. To be removed in 3.6.0 at the earliest.
	 */
	@Deprecated
	public static final boolean isInstrumentationAvailable() {
		return isMicrometerAvailable;
	}

	/**
	 * @deprecated Prefer using the reactor-core-micrometer module and configuring it using the Micrometer entrypoint.
	 */
	@Deprecated
	public static class MicrometerConfiguration {

		private static MeterRegistry registry = globalRegistry;

		/**
		 * Set the registry to use in reactor for metrics related purposes.
		 * <p>
		 * This is only used by the deprecated inline Micrometer instrumentation, and not by the reactor-core-micrometer
		 * module.
		 * 
		 * @return the previously configured registry.
		 * @deprecated prefer using Micrometer setup in new reactor-core-micrometer module. To be removed at the earliest in 3.6.0.
		 */
		@Deprecated
		public static MeterRegistry useRegistry(MeterRegistry registry) {
			MeterRegistry previous = MicrometerConfiguration.registry;
			MicrometerConfiguration.registry = registry;
			return previous;
		}

		/**
		 * Get the registry used in reactor for metrics related purposes.
		 * <p>
		 * This is only reflecting the deprecated inline Micrometer instrumentation configuration, and not the configuration
		 * of the reactor-core-micrometer module.
		 *
		 * @return the configured registry
		 * @see Flux#metrics()
		 * @deprecated prefer using Micrometer setup in new reactor-core-micrometer module. To be removed at the earliest in 3.6.0.
		 */
		@Deprecated
		public static MeterRegistry getRegistry() {
			return registry;
		}
	}
}
