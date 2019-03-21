/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class SerializedSinkTest {

	@Test
	public void raceBetweenNext() {
		for (int i = 0; i < 1000; i++) {

			AssertSubscriber<String> ts = AssertSubscriber.create();
			FluxCreate.BaseSink<String> baseSink = FluxCreate.createSink(ts, FluxSink.OverflowStrategy.BUFFER);
			FluxCreate.SerializedSink<String> sink = new FluxCreate.SerializedSink<>(baseSink);

			baseSink.request(Long.MAX_VALUE);

			sink.next("foo");
			RaceTestUtils.race(() -> sink.next("bar"),
					() -> sink.next("baz"));

			sink.complete();

			Set<String> expected = new HashSet<String>() {{
				add("foo");
				add("bar");
				add("baz");
			}};

			ts.assertContainValues(expected);
			ts.assertComplete();
		}
	}

	@Test
	@Parameters(source = FluxSink.OverflowStrategy.class)
	public void raceBetweenNextAndTryNext(FluxSink.OverflowStrategy STRATEGY) {
		final int NUM_LOOPS = 1000;
		final AtomicInteger trySuccess = new AtomicInteger();
		final AtomicInteger tryRejected = new AtomicInteger();

		int overflowAccepted = 0;
		int overflowRejected = 0;

		for (int i = 0; i < NUM_LOOPS; i++) {
			AssertSubscriber<String> ts = AssertSubscriber.create();
			FluxCreate.BaseSink<String> baseSink = FluxCreate.createSink(ts, STRATEGY);
			FluxCreate.SerializedSink<String> sink = new FluxCreate.SerializedSink<>(baseSink);

			AtomicBoolean bazAccepted = new AtomicBoolean();

			baseSink.request(3);

			sink.next("foo");
			RaceTestUtils.race(
					() -> {
						boolean tried = sink.tryNext("baz");
						if (tried) trySuccess.incrementAndGet();
						else tryRejected.incrementAndGet();
						bazAccepted.set(tried);
					},
					() -> sink.next("bar")
			);

			boolean overflow1 = sink.tryNext("overflow1");
			boolean overflow2 = sink.tryNext("overflow2");

			sink.complete();

			if (bazAccepted.get()) {
				assertThat(overflow1).isFalse();
			}
			else {
				assertThat(overflow1).isTrue();
			}
			assertThat(overflow2).isFalse();

			if (overflow1) overflowAccepted++;
			else overflowRejected++;

			if (overflow2) overflowAccepted++;
			else overflowRejected++;

			Set<String> expected = new HashSet<String>() {{
				add("foo");
				add("bar");
			}};
			if (bazAccepted.get()) {
				expected.add("baz");
			}

			ts.assertContainValues(expected);
			ts.assertComplete();
		}

		String racingDesc = "racing tryNext(" + STRATEGY + ")[accepted=" + trySuccess.get() + ", rejected=" + tryRejected.get() + "]";
		String overflowDesc = "overflow tryNext(" + STRATEGY + ")[accepted=" + overflowAccepted + ", rejected=" + overflowRejected + "]";
		System.out.println(racingDesc + "\t" + overflowDesc) ;

		assertThat(trySuccess.get() + tryRejected.get())
				.as(racingDesc)
				.isEqualTo(NUM_LOOPS);

		assertThat(overflowAccepted + overflowRejected)
				.as(overflowDesc)
				.isEqualTo(NUM_LOOPS * 2);
	}

}