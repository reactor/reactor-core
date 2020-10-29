/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoCollectMapTest {

	static final class Pojo {

		final String name;
		final long   id;

		Pojo(String name, long id) {
			this.name = name;
			this.id = id;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Pojo pojo = (Pojo) o;

			return id == pojo.id;
		}

		@Override
		public int hashCode() {
			return (int) (id ^ (id >>> 32));
		}
	}

	@Test
	public void collectMap() {
		StepVerifier.create(Flux.just(new Pojo("test", 1L),
				new Pojo("test", 2L),
				new Pojo("test2", 3L))
		                        .collectMap(p -> p.id))
		            .assertNext(d -> assertThat(d).containsKeys(1L, 2L, 3L)
		                                          .containsValues(new Pojo("test", 1L),
				                                          new Pojo("test", 2L),
				                                          new Pojo("test2", 3L)))
		            .verifyComplete();

	}

	@Test
	public void collectMapEmpty() {
		StepVerifier.create(Flux.<Pojo>empty().collectMap(p -> p.id))
		            .assertNext(d -> assertThat(d).isEmpty())
		            .verifyComplete();

	}

	@Test
	public void collectMapCallable() {
		StepVerifier.create(Mono.fromCallable(() -> new Pojo("test", 1L))
		                        .flux()
		                        .collectMap(p -> p.id))
		            .assertNext(p -> assertThat(p).containsOnlyKeys(1L)
		                                          .containsValues(new Pojo("test", 1L)))
		            .verifyComplete();

	}

	@Test
	public void collectMultiMap() {
		StepVerifier.create(Flux.just(new Pojo("test", 1L),
				new Pojo("test", 2L),
				new Pojo("test2", 3L))
		                        .collectMultimap(p -> p.name))
		            .assertNext(d -> assertThat(d).containsKeys("test", "test2")
		                                          .containsValues(Arrays.asList(new Pojo(
						                                          "test",
						                                          1L), new Pojo("test", 2L)),
				                                          Arrays.asList(new Pojo("test2",
						                                          3L))))
		            .verifyComplete();

	}

	@Test
	public void collectMultiMapEmpty() {
		StepVerifier.create(Flux.<Pojo>empty().collectMultimap(p -> p.id))
		            .assertNext(d -> assertThat(d).isEmpty())
		            .verifyComplete();

	}

	@Test
	public void collectMultiMapCallable() {
		StepVerifier.create(Mono.fromCallable(() -> new Pojo("test", 1L))
		                        .flux()
		                        .collectMultimap(p -> p.id))
		            .assertNext(p -> assertThat(p).containsOnlyKeys(1L)
		                                          .containsValues(Arrays.asList(new Pojo(
				                                          "test",
				                                          1L))))
		            .verifyComplete();

	}
}
