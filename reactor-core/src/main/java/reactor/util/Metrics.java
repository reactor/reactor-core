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
 * Deprecated as of 3.5.0, prefer using the new reactor-metrics-micrometer module.
 *
 * @author Simon Baslé
 * @deprecated prefer using the new reactor-metrics-micrometer module Micrometer entrypoint. To be removed in 3.6.0 at the earliest.
 */
@Deprecated
public class Metrics {

	static final boolean isMicrometerAvailable;
	static final boolean isMicrometerReactorModuleAvailable;

	static {
		boolean micrometer;
		boolean micrometerModule;
		try {
			globalRegistry.getRegistries();
			micrometer = true;
		}
		catch (Throwable t) {
			micrometer = false;
		}
		try {
			Metrics.class.getClassLoader().loadClass("reactor.metrics.micrometer.Micrometer");
			micrometerModule = true;
		}
		catch (Throwable t) {
			micrometerModule = false;
		}
		isMicrometerAvailable = micrometer;
		isMicrometerReactorModuleAvailable = micrometerModule;
	}

	/**
	 * Check if the current runtime supports metrics / instrumentation, by
	 * verifying if Micrometer is on the classpath.
	 * <p>
	 * Note that as part of the migration path to the reactor-metrics-micrometer module, if said module is
	 * also on the classpath this method will return {@literal false} (and the {@code metrics()} operator will be
	 * NO-OP).
	 *
	 * @return true if the Micrometer instrumentation facade is available, unless the reactor-metrics-micrometer module is also on the classpath
	 * @deprecated prefer explicit usage of the reactor-metrics-micrometer module. To be removed in 3.6.0 at the earliest.
	 */
	@Deprecated
	public static final boolean isInstrumentationAvailable() {
		return isMicrometerAvailable && !isMicrometerReactorModuleAvailable;
	}

	/**
	 * @deprecated Prefer using the reactor-metrics-micrometer module and configuring it using the Micrometer entrypoint.
	 */
	@Deprecated
	public static class MicrometerConfiguration {

		private static MeterRegistry registry = globalRegistry;

		/**
		 * Set the registry to use in reactor for metrics related purposes.
		 * 
		 * @return the previously configured registry.
		 * @throws UnsupportedOperationException if the method is used while the reactor-metrics-module is on the classpath (this registry is never used in that case)
		 * @deprecated prefer using Micrometer setup in new reactor-metrics-micrometer module. To be removed at the earliest in 3.6.0.
		 */
		@Deprecated
		public static MeterRegistry useRegistry(MeterRegistry registry) {
			if (Metrics.isMicrometerReactorModuleAvailable) {
				throw new UnsupportedOperationException("When reactor-metrics-micrometer is on the classpath, the MicrometerConfiguration registry is never used");
			}
			MeterRegistry previous = MicrometerConfiguration.registry;
			MicrometerConfiguration.registry = registry;
			return previous;
		}

		/**
		 * Get the registry used in reactor for metrics related purposes.
		 *
		 * @return the configured registry
		 * @throws UnsupportedOperationException if the method is used while the reactor-metrics-module is on the classpath (this registry is never used in that case)
		 * @see Flux#metrics()
		 * @deprecated prefer using Micrometer setup in new reactor-metrics-micrometer module. To be removed at the earliest in 3.6.0.
		 */
		@Deprecated
		public static MeterRegistry getRegistry() {
			if (Metrics.isMicrometerReactorModuleAvailable) {
				throw new UnsupportedOperationException("When reactor-metrics-micrometer is on the classpath, the MicrometerConfiguration registry is never used");
			}
			return registry;
		}
	}
}
