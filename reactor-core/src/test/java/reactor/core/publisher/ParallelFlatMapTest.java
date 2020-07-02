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

package reactor.core.publisher;

import org.junit.Test;
import reactor.core.Scannable;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelFlatMapTest {

	@Test
	public void parallelism() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelFlatMap<Integer, Integer> test = new ParallelFlatMap<>(source,
				i -> Flux.range(1, i), false, 12,
				Queues.small(), 123, Queues.small());

		assertThat(test.parallelism())
				.isEqualTo(3)
				.isEqualTo(source.parallelism());
	}

	@Test
	public void scanOperator() throws Exception {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelFlatMap<Integer, Integer> test = new ParallelFlatMap<>(source,
				i -> Flux.range(1, i), true, 12,
				Queues.small(), 123, Queues.small());

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH))
				.isEqualTo(123)
				.isNotEqualTo(source.getPrefetch());
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}