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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Utility class around metrics, useful to programmatically reference meters and detect
 * if Micrometer is on the classpath. For now hidden from the public API.
 *
 * @author Simon Baslé
 */
final class Metrics {

	private static final boolean isMicrometerAvailable;

	static {
		boolean micrometer;
		try {
			io.micrometer.core.instrument.Metrics.globalRegistry.getRegistries();
			micrometer = true;
		}
		catch (Throwable t) {
			micrometer = false;
		}
		isMicrometerAvailable = micrometer;
	}

	/**
	 * @return true if the Micrometer instrumentation facade is available
	 */
	public static final boolean isMicrometerAvailable() {
		return isMicrometerAvailable;
	}

	//=== Constants ===

	/**
	 * The default sequence name that will be used for instrumented {@link Flux} and
	 * {@link Mono} that don't have a {@link Flux#name(String) name}.
	 *
	 * @see #TAG_SEQUENCE_NAME
	 */
	public static final String REACTOR_DEFAULT_NAME = "reactor";

	//Note: meters and tag names are normalized by micrometer on the basis that the word
	// separator is the dot, not camelCase...
	/**
	 * Meter that counts the number of events received from a malformed source (ie an
	 * onNext after an onComplete).
	 */
	public static final String METER_MALFORMED              = "reactor.malformed.source";
	/**
	 * Meter that counts the number of subscriptions to a sequence.
	 */
	public static final String METER_SUBSCRIBED             = "reactor.subscribed";
	/**
	 * Meter that times the duration between the subscription and the sequence's terminal
	 * event. The timer is also using the {@link #TAG_TERMINATION_TYPE} tag to determine
	 * which kind of event terminated the sequence.
	 */
	public static final String METER_SUBSCRIBE_TO_TERMINATE = "reactor.subscribe.to.terminate";
	/**
	 * Meter that times the delays between each onNext (or between the first onNext and
	 * the onSubscribe event).
	 */
	public static final String METER_ON_NEXT_DELAY          = "reactor.onNext.delay";
	/**
	 * Meter that tracks the request amount, in {@link Flux#name(String) named}
	 * sequences only.
	 */
	public static final String METER_REQUESTED              = "reactor.requested";

	/**
	 * Tag used by {@link #METER_SUBSCRIBE_TO_TERMINATE} to mark what kind of terminating
	 * event occurred: {@link #TAGVALUE_ON_COMPLETE}, {@link #TAGVALUE_ON_ERROR} or
	 * {@link #TAGVALUE_CANCEL}.
	 */
	public static final String TAG_TERMINATION_TYPE = "reactor.termination.type";
	/**
	 * Tag bearing the sequence's name, as given by the {@link Flux#name(String)} operator.
	 */
	public static final String TAG_SEQUENCE_NAME    = "reactor.sequence.name";
	/**
	 * Tag bearing the sequence's type, {@link Flux} or {@link Mono}.
	 * @see #TAGVALUE_FLUX
	 * @see #TAGVALUE_MONO
	 */
	public static final String TAG_SEQUENCE_TYPE    = "reactor.sequence.type";

	//... tag values are free-for-all
	public static final String TAGVALUE_ON_ERROR    = "onError";
	public static final String TAGVALUE_ON_COMPLETE = "onComplete";
	public static final String TAGVALUE_CANCEL      = "cancel";
	public static final String TAGVALUE_FLUX        = "Flux";
	public static final String TAGVALUE_MONO        = "Mono";

}
