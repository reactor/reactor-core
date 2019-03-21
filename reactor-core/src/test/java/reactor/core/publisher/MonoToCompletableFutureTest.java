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

import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class MonoToCompletableFutureTest {

	@Test
	public void normal() throws Exception {
		CompletableFuture<Integer> f = Mono.just(1)
		                                   .toFuture();

		assertThat(f.get()).isEqualTo(1);
	}

	@Test(expected = Exception.class)
	public void error() throws Exception {
		CompletableFuture<Integer> f =
				Mono.<Integer>error(new Exception("test")).toFuture();

		assertThat(f.isDone()).isTrue();
		assertThat(f.isCompletedExceptionally()).isTrue();
		f.get();
	}

	@Test
	public void empty() throws Exception {
		CompletableFuture<Integer> f = Mono.<Integer>empty().toFuture();

		assertThat(f.get()).isNull();
	}
}