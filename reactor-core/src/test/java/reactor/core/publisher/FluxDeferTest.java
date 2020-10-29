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
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;

public class FluxDeferTest {

	@Test
	public void supplierNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.<Integer>defer(null);
		});
	}

	@Test
	public void supplierReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>defer(() -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void supplierThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>defer(() -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.defer(() -> Flux.just(1)).subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}


	@Test
	public void deferStream(){
		AtomicInteger i = new AtomicInteger();

		Flux<Integer> source =
				Flux.defer(() -> Flux.just(i.incrementAndGet()));

		assertThat(source.blockLast().intValue()).isEqualTo(1);
		assertThat(source.blockLast().intValue()).isEqualTo(2);
		assertThat(source.blockLast().intValue()).isEqualTo(3);
	}

	@Test
	public void deferFluxWithContext() {
		Flux<Integer> source = Flux
				.deferContextual(ctx -> {
					AtomicInteger i = ctx.get("i");
					return Mono.just(i.incrementAndGet());
				})
				.contextWrite(Context.of(
						"i", new AtomicInteger()
				));

		assertThat(source.blockLast().intValue()).isEqualTo(1);
		assertThat(source.blockLast().intValue()).isEqualTo(2);
		assertThat(source.blockLast().intValue()).isEqualTo(3);
	}

	@Test
	public void scanOperator(){
	    FluxDefer<Integer> test = new FluxDefer<>(() -> Flux.just(1));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
