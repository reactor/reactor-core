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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.FieldLayout;

import static org.assertj.core.api.Assertions.assertThat;

public class QueueDrainSubscriberTest {

	@Test
	public void objectPadding() {
		ClassLayout layout = ClassLayout.parseClass(QueueDrainSubscriber.class);
		AtomicReference<FieldLayout> wip = new AtomicReference<>();
		AtomicReference<FieldLayout> requested = new AtomicReference<>();

		layout.fields().forEach(f -> {
			if ("wip".equals(f.name())) wip.set(f);
			else if ("requested".equals(f.name())) requested.set(f);
		});

		final FieldLayout fieldAfterRequested = layout.fields()
		                                              .tailSet(requested.get())
		                                              .stream()
		                                              .skip(1)
		                                              .filter(fl -> fl.name().length() >= 4)
		                                              .findFirst()
		                                              .get();

		assertThat(layout.fields().headSet(wip.get()))
				.as("wip pre-padding")
				.hasSize(15)
				.allSatisfy(fl -> assertThat(fl.name()).startsWith("p"));

		assertThat(layout.fields().subSet(wip.get(), requested.get()).stream().skip(1))
				.as("wip-requested padding")
				.hasSize(15)
				.allSatisfy(fl -> assertThat(fl.name()).startsWith("p").endsWith("a"));

		assertThat(layout.fields().subSet(requested.get(), fieldAfterRequested)
		                 .stream()
		                 .skip(1))
				.as("requested post-padding")
				.hasSize(15)
				.allSatisfy(fl -> assertThat(fl.name()).startsWith("q").isNotEqualTo("queue"));

		assertThat(wip.get().offset())
				.as("wip offset")
				.isEqualTo(136);
		assertThat(requested.get().offset())
				.as("requested offset")
				.isEqualTo(wip.get().offset() + 128);

		System.out.println(wip.get());
		System.out.println(requested.get());
		System.out.println(fieldAfterRequested);
	}

}
