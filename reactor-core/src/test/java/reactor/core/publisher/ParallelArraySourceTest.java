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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Flux.just;

public class ParallelArraySourceTest {

	@Test
	public void parallelism() {
		@SuppressWarnings("unchecked")
		Publisher<Integer>[] sources = new Publisher[2];
		sources[0] = Flux.range(1, 4);
		sources[1] = just(10);
		ParallelArraySource<Integer> test = new ParallelArraySource<>(sources);

		assertThat(test.parallelism()).isEqualTo(2);
	}

	@Test
	public void scanOperator(){
		@SuppressWarnings("unchecked")
		Publisher<Integer>[] sources = new Publisher[2];
		sources[0] = Flux.range(1, 4);
		sources[1] = just(10);
		ParallelArraySource<Integer> test = new ParallelArraySource<>(sources);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
