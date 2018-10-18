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

	private static final boolean isMicrometerAvailable;

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
	static MeterRegistry defaultRegistry;

	/**
	 * @return true if the Micrometer instrumentation facade is available
	 */
	public static final boolean isMicrometerAvailable() {
		return isMicrometerAvailable;
	}

	private static final void resetRegistry() {
		MeterRegistry old = Metrics.defaultRegistry;
		Metrics.defaultRegistry = null;
		if (old != null) {
			old.close();
		}
	}

	/**
	 * Define a Micrometer MeterRegistry, passed as a simple {@link Object}, to be used by
	 * instrumentation-related features of Reactor. If Micrometer is not available on the
	 * classpath OR the passed object is not a registry, returns {@code false} and does nothing.
	 * <p>
	 * If you have already defined a valid default meter registry previously, said registry
	 * will be closed on your behalf.
	 *
	 * @implNote The candidate registry is exposed as an Object in order to avoid a hard
	 * dependency of Reactor on Micrometer.
	 *
	 * @param registryCandidate the {@link Object} that is potentially a Micrometer registry,
	 * or null to reset to the default of using the Micrometer global registry.
	 * @return true if the object was a registry (and thus Micrometer is available), false otherwise.
	 */
	public static boolean setRegistryCandidate(@Nullable Object registryCandidate) {
		if (registryCandidate == null) {
			resetRegistry();
			return true;
		}
		if (isMicrometerAvailable && registryCandidate instanceof MeterRegistry) {
			resetRegistry();
			Metrics.defaultRegistry = (MeterRegistry) registryCandidate;
			return true;
		}
		return false;
	}

	/**
	 * Return a candidate Micrometer registry. If {@link #setRegistryCandidate(Object)} was
	 * called with a valid registry instance, this method returns said instance. Otherwise
	 * it returns {@code null}.
	 *
	 * @implNote The candidate registry is exposed as an Object in order to avoid a hard
	 * dependency of Reactor on Micrometer.
	 *
	 * @return the default Micrometer registry to be used by Reactor, or null if not set / not available.
	 */
	@Nullable
	public static Object getRegistryCandidate() {
		return defaultRegistry;
	}

}
