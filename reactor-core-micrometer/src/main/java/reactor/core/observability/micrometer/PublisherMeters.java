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

/**
 * Meters and tags used by {@link MicrometerMeterListener}.
 */
public enum PublisherMeters implements DocumentedMeter {

	/**
	 * Counts the number of events received from a malformed source (ie an onNext after an onComplete).
	 */
	MALFORMED_SOURCE_EVENTS {
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
			return "%s.onNext.delay";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}
	},

	/**
	 * Counts the amount requested to a named sequence (eg.  Flux.name(String)) by all subscribers, until at least one requests an unbounded amount.
	 */
	REQUESTED_AMOUNT {
		@Override
		public KeyName[] getKeyNames() {
			return CommonTags.values();
		}

		@Override
		public String getName() {
			return "%s.requested";
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
			return "%s.subscribed";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.COUNTER;
		}
	},

	/**
	 * Times the duration elapsed between a subscription and the termination or cancellation of the sequence.
	 * A TerminationTags#STATUS tag is added to specify what event caused the timer to end (completed, completedEmpty, error, cancelled).
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
			return "%s.flow.duration";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.DISTRIBUTION_SUMMARY;
		}
	};

	/**
	 * Tags that are common to all PublisherMeters.
	 */
	public enum CommonTags implements KeyName {

		/**
		 * The type of the sequence (Flux or Mono).
		 */
		TYPE {
			@Override
			public String asString() {
				return "type";
			}
		};

		/**
		 * TYPE for reactor.core.publisher.Flux
		 */
		public static final String TAG_TYPE_FLUX = "Flux";
		/**
		 * TYPE for reactor.core.publisher.Mono
		 */
		public static final String TAG_TYPE_MONO = "Mono";

	}

	/**
	 * Additional tags for PublisherMeters#FLOW_DURATION that reflects the termination
	 * of the sequence.
	 */
	public enum TerminationTags implements KeyName {

		/**
		 * The termination status:
		 * <ul>
		 *     <li>TAG_STATUS_COMPLETED for a sequence that terminates with an onComplete, with onNext(s)</li>
		 *     <li>TAG_STATUS_COMPLETED_EMPTY for a sequence that terminates without any onNext before the onComplete</li>
		 *     <li>TAG_STATUS_ERROR for a sequence that terminates with an onError</li>
		 *     <li>TAG_STATUS_CANCELLED for a sequence that has cancelled its subscription</li>
		 * </ul>
		 */
		STATUS {
			@Override
			public String asString() {
				return "status";
			}
		},

		/**
		 * Tag used by FLOW_DURATION when STATUS is TAG_STATUS_ERROR, to store the
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
		 * Status for a sequence that terminates with an onComplete, with onNext(s).
		 */
		public static final String TAG_STATUS_COMPLETED       = "completed";
		/**
		 * Status for a sequence that terminates without any onNext before the onComplete.
		 */
		public static final String TAG_STATUS_COMPLETED_EMPTY = "completedEmpty";
		/**
		 * Status for a sequence that terminates with an onError.
		 */
		public static final String TAG_STATUS_ERROR           = "error";

	}
}
