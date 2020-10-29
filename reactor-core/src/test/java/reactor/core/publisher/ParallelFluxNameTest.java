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

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelFluxNameTest {

	@Test
	public void parallelism() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelFluxName<Integer> test = new ParallelFluxName<>(source, "foo", null);

		assertThat(test.parallelism())
				.isEqualTo(3)
				.isEqualTo(source.parallelism());
	}

	@Test
	public void scanOperator() throws Exception {
		Tuple2<String, String> tag1 = Tuples.of("foo", "oof");
		Tuple2<String, String> tag2 = Tuples.of("bar", "rab");
		Set<Tuple2<String, String>> tags = new HashSet<>();
		tags.add(tag1);
		tags.add(tag2);

		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelFluxName<Integer> test = new ParallelFluxName<>(source, "foo", tags);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH))
				.isEqualTo(256)
				.isEqualTo(source.getPrefetch());

		assertThat(test.scan(Scannable.Attr.NAME)).isEqualTo("foo");
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		final Stream<Tuple2<String, String>> scannedTags = test.scan(Scannable.Attr.TAGS);
		assertThat(scannedTags).isNotNull();
		assertThat(scannedTags.iterator()).containsExactlyInAnyOrder(tag1, tag2);
	}

	@Test
	public void scanOperatorNullTags() throws Exception {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelFluxName<Integer> test = new ParallelFluxName<>(source, "foo", null);

		assertThat(test.scan(Scannable.Attr.TAGS)).isNull();
	}

}
