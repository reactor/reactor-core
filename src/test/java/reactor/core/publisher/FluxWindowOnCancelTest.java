/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import org.junit.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class FluxWindowOnCancelTest {

	@Test
	public void normal() throws Exception {
		StepVerifier.create(Flux.just("red", "white", "blue")
		                 .window()
		                 .concatMap(w -> w.take(1).collectList()))
		            .assertNext(s -> assertThat(s.get(0)).isEqualTo("red"))
		            .assertNext(s -> assertThat(s.get(0)).isEqualTo("white"))
		            .assertNext(s -> assertThat(s.get(0)).isEqualTo("blue"))
	                .verifyComplete();

	}
}