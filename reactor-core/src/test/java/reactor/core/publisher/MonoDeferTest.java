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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.util.context.Context;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoDeferTest {

	@Test
	public void deferMono(){
		AtomicInteger i = new AtomicInteger();

		Mono<Integer> source =
				Mono.defer(() -> Mono.just(i.incrementAndGet()));

		assertThat(source.block().intValue()).isEqualTo(1);
		assertThat(source.block().intValue()).isEqualTo(2);
		assertThat(source.block().intValue()).isEqualTo(3);
	}

	@Test
	public void deferMonoWithContext() {
		Mono<Integer> source = Mono
				.deferContextual(ctx -> {
					AtomicInteger i = ctx.get("i");
					return Mono.just(i.incrementAndGet());
				})
				.contextWrite(Context.of(
						"i", new AtomicInteger()
				));

		assertThat(1).isEqualTo(source.block().intValue());
		assertThat(2).isEqualTo(source.block().intValue());
		assertThat(3).isEqualTo(source.block().intValue());
	}

	@Test
	public void scanOperator() {
		AtomicInteger i = new AtomicInteger();

		MonoDefer<Integer> test = new MonoDefer<>(() -> Mono.just(i.incrementAndGet()));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
	}

	@Test
	public void scanOperatorWithContext() {
		AtomicInteger i = new AtomicInteger();

		MonoDeferContextual<Integer> test = new MonoDeferContextual<>(c -> Mono.just(i.incrementAndGet()));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
	}
}