/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.*;

public class ParallelDoOnEachTest {

	@Test
	public void scanOperator(){
		ParallelFlux<Integer> parent = Flux.just(1).parallel(2);
		ParallelDoOnEach<Integer> test = new ParallelDoOnEach<>(parent, (k, v) -> {}, (k, v) -> {}, v -> {});

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Queues.SMALL_BUFFER_SIZE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}