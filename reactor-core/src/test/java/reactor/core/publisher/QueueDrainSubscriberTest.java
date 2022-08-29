/*
 * Copyright (c) 2018-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.ClassLayout;

import static org.assertj.core.api.Assertions.assertThat;

public class QueueDrainSubscriberTest {

	@Test
	@Tag("slow")
	void objectPadding() {
		ClassLayout layout = ClassLayout.parseClass(QueueDrainSubscriber.class);

		AtomicLong currentPaddingSize = new AtomicLong();
		List<String> fields = new ArrayList<>();
		List<Long> paddingSizes = new ArrayList<>();

		layout.fields().forEach(f -> {
			if (f.name().startsWith("pad")) {
				currentPaddingSize.addAndGet(f.size());
			} else {
				if (currentPaddingSize.get() > 0) {
					fields.add("[padding]");
					paddingSizes.add(currentPaddingSize.getAndSet(0));
				}
				fields.add(f.name());
			}
		});
		if (currentPaddingSize.get() > 0) {
			fields.add("[padding]");
			paddingSizes.add(currentPaddingSize.getAndSet(0));
		}

		assertThat(fields).containsExactly(
				"[padding]",
				"wip",
				"[padding]",
				"requested",
				"[padding]",
				"cancelled",
				"done",
				"actual",
				"queue",
				"error"
		);

		assertThat(paddingSizes).containsExactly(128L, 128L, 128L);
	}
}
