/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.ConnectableFlux;

/**
 * Test utilities that helps with mocking.
 *
 * @author Simon Baslé
 */
public class MockUtils {

	/**
	 * An abstract class that can be used to mock a {@link Scannable} {@link ConnectableFlux}.
	 */
	public static abstract class TestScannableConnectableFlux<T>
			extends ConnectableFlux<T>
			implements Scannable { }

	/**
	 * An interface that can be used to mock a {@link Scannable}
	 * {@link reactor.core.Fuseable.ConditionalSubscriber}.
	 */
	public interface TestScannableConditionalSubscriber<T>
			extends Fuseable.ConditionalSubscriber<T>,
			        Scannable { }

	/**
	 * A {@link Clock} that can be manipulated, to be used in tests.
	 */
	public static final class VirtualClock extends Clock {

		private       Instant instant;
		private final ZoneId  zone;

		public VirtualClock(Instant initialInstant, ZoneId zone) {
			this.instant = initialInstant;
			this.zone = zone;
		}

		public VirtualClock() {
			this(Instant.EPOCH, ZoneId.systemDefault());
		}

		public void setInstant(Instant newFixedInstant) {
			this.instant = newFixedInstant;
		}

		public void advanceTimeBy(Duration duration) {
			this.instant = this.instant.plus(duration);
		}

		@Override
		public ZoneId getZone() {
			return zone;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			if (zone.equals(this.zone)) {  // intentional NPE
				return this;
			}
			return new VirtualClock(instant, zone);
		}
		@Override
		public long millis() {
			return instant.toEpochMilli();
		}

		@Override
		public Instant instant() {
			return instant;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof VirtualClock) {
				VirtualClock other = (VirtualClock) obj;
				return instant.equals(other.instant) && zone.equals(other.zone);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return instant.hashCode() ^ zone.hashCode();
		}

		@Override
		public String toString() {
			return "VirtualClock[" + instant + "," + zone + "]";
		}
	}
}
