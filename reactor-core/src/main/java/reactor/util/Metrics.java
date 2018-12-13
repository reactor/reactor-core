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

	//intentionally an Object, so that even with eager loading of fields this won't blow up in NoClassDefFoundError
	@Nullable
	private static Object defaultRegistry;

	/**
	 * Check if the current runtime supports metrics / instrumentation, by
	 * verifying if Micrometer is on the classpath.
	 *
	 * @return true if the Micrometer instrumentation facade is available
	 */
	public static final boolean isInstrumentationAvailable() {
		return isMicrometerAvailable;
	}

	/**
	 * Attempt to set up a custom default meter registry for the case where {@link #isInstrumentationAvailable()}.
	 * <p>
	 * To avoid referring to micrometer classes, this method takes an arbitrary {@link Object},
	 * and is thus type-unsafe. The candidate is only accepted if it is a suitable {@code MeterRegistry},
	 * provided instrumentation is indeed available. Otherwise it is ignored and this method
	 * returns false.
	 *
	 * @param registryCandidate the candidate {@link Object} to set up as meter registry.
	 * Use {@literal null} to reset.
	 * @return true if the candidate was accepted, false otherwise.
	 */
	public static final boolean setUnsafeRegistry(@Nullable Object registryCandidate) {
		if (isMicrometerAvailable) {
			if (registryCandidate == null || registryCandidate instanceof MeterRegistry) {
				defaultRegistry = registryCandidate;
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns a meter registry IF {@link #isInstrumentationAvailable()} (either the global
	 * one or a suitable registry that has been set via {@link #setUnsafeRegistry(Object)}),
	 * or null otherwise. If the value is not null, it can always be casted to a {@code MeterRegistry}.
	 * <p>
	 * To avoid referring to micrometer classes, this method returns an {@link Object}, and
	 * is thus type-unsafe.
	 *
	 * @return an {@link Object} that is a suitable default meter registry if relevant, null otherwise.
	 */
	@Nullable
	public static final Object getUnsafeRegistry() {
		if (isMicrometerAvailable) {
			return defaultRegistry != null ? defaultRegistry : globalRegistry;
		}
		return null;
	}
}
