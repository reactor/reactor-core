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

import io.micrometer.core.instrument.MeterRegistry;

import reactor.util.annotation.Nullable;

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

	@Nullable
	private static MeterRegistry defaultRegistry;

	/**
	 * Check if the current runtime supports metrics / instrumentation, by
	 * verifying if Micrometer is on the classpath.
	 *
	 * @return true if the Micrometer instrumentation facade is available
	 */
	public static final boolean isInstrumentationAvailable() {
		return isMicrometerAvailable;
	}

	public static final boolean setUnsafeRegistry(@Nullable Object registryCandidate) {
		if (isMicrometerAvailable) {
			if (registryCandidate == null || registryCandidate instanceof MeterRegistry) {
				defaultRegistry = (MeterRegistry) registryCandidate;
				return true;
			}
		}
		return false;
	}

	@Nullable
	public static final Object getUnsafeRegistry() {
		if (isMicrometerAvailable) {
			return defaultRegistry == null ? globalRegistry : defaultRegistry;
		}
		return null;
	}
}
