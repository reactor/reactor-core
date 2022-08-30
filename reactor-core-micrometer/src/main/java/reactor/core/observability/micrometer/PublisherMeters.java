/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.observability.micrometer;

import io.micrometer.common.docs.KeyName;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.docs.DocumentedMeter;
import org.reactivestreams.Subscriber;

import reactor.core.publisher.Flux;

/**
 * Meters and tags used by {@link MicrometerMeterListener}.
 */
public enum PublisherMeters implements DocumentedMeter {

	/**
	 * Counts the number of events received from a malformed source (ie an onNext after an onComplete).
	 */
	MALFORMED {
		@Override
		public KeyName[] getKeyNames() {
			return CommonTags.values();
		}

		@Override
		public String getName() {
			return "%s.malformed.source";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.COUNTER;
		}
	},

	/**
	 * Measures the delay between each onNext (or between the first onNext and the onSubscribe event).
	 */
	ON_NEXT_DELAY {
		@Override
		public String getBaseUnit() {
			return ""; //FIXME nanoseconds? milliseconds
		}

		@Override
		public KeyName[] getKeyNames() {
			return CommonTags.values();
		}

		@Override
		public String getName() {
			return "%s" + ".onNext.delay";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}
	},

	/**
	 * Counts the amount requested to a {@link Flux#name(String) named} sequence by all subscribers, until at least one requests an unbounded amount.
	 */
	REQUESTED {
		@Override
		public KeyName[] getKeyNames() {
			return CommonTags.values();
		}

		@Override
		public String getName() {
			return "%s" + ".requested";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.DISTRIBUTION_SUMMARY;
		}
	},

	/**
	 * Counts the number of subscriptions to a sequence.
	 */
	SUBSCRIBED {
		@Override
		public KeyName[] getKeyNames() {
			return CommonTags.values();
		}

		@Override
		public String getName() {
			return "%s" + ".subscribed";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.COUNTER;
		}
	},

	/**
	 * Times the duration elapsed between a subscription and the termination or cancellation of the sequence.
	 * A {@link TerminationTags#STATUS status} tag is added to specify what event caused the timer to end (completed, completedEmpty, error, cancelled).
	 */
	FLOW_DURATION {
		@Override
		public KeyName[] getKeyNames() {
			return KeyName.merge(
				CommonTags.values(),
				TerminationTags.values()
			);
		}

		@Override
		public String getName() {
			return "%s" + ".flow.duration";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.DISTRIBUTION_SUMMARY;
		}
	};

//

	/**
	 * Tags that are common to all {@link PublisherMeters}.
	 */
	public enum CommonTags implements KeyName {

		/**
		 * The type of the sequence ({@link #TAG_TYPE_FLUX Flux} or {@link #TAG_TYPE_MONO Mono}).
		 */
		TYPE {
			@Override
			public String asString() {
				return "type";
			}
		};

		/**
		 * {@link #TYPE} for {@link reactor.core.publisher.Flux}
		 */
		public static final String TAG_TYPE_FLUX = "Flux";
		/**
		 * {@link #TYPE} for {@link reactor.core.publisher.Mono}
		 */
		public static final String TAG_TYPE_MONO = "Mono";

	}

	/**
	 * Additional tags for {@link PublisherMeters#FLOW_DURATION} that reflects the termination
	 * of the sequence.
	 */
	public enum TerminationTags implements KeyName {

		/**
		 * The termination status:
		 * <ul>
		 *     <li>{@link #TAG_STATUS_COMPLETED} for a sequence that terminates with an {@link Subscriber#onComplete() onComplete}, with onNext(s)</li>
		 *     <li>{@link #TAG_STATUS_COMPLETED_EMPTY} for a sequence that terminates without any onNext before the {@link Subscriber#onComplete() onComplete}</li>
		 *     <li>{@link #TAG_STATUS_ERROR} for a sequence that terminates with an {@link Subscriber#onError(Throwable) onError}</li>
		 *     <li>{@link #TAG_STATUS_CANCELLED} for a sequence that has cancelled its subscription</li>
		 * </ul>
		 */
		STATUS {
			@Override
			public String asString() {
				return "status";
			}
		},

		/**
		 * Tag used by {@link #FLOW_DURATION} when {@link #STATUS} is {@link #TAG_STATUS_ERROR}, to store the
		 * exception that occurred.
		 */
		EXCEPTION {
			@Override
			public String asString() {
				return "exception";
			}
		};

		//Status
		/**
		 * Status for a sequence that has cancelled its subscription.
		 */
		public static final String TAG_STATUS_CANCELLED       = "cancelled";
		/**
		 * Status for a sequence that terminates with an {@link Subscriber#onComplete() onComplete}, with onNext(s).
		 */
		public static final String TAG_STATUS_COMPLETED       = "completed";
		/**
		 * Status for a sequence that terminates without any onNext before the {@link Subscriber#onComplete() onComplete}.
		 */
		public static final String TAG_STATUS_COMPLETED_EMPTY = "completedEmpty";
		/**
		 * Status for a sequence that terminates with an {@link Subscriber#onError(Throwable) onError}.
		 */
		public static final String TAG_STATUS_ERROR           = "error";

	}
}
