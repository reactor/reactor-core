/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicInteger;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxErrorSuppliedTest {

	@Test
	public void normal() {
		StepVerifier.create(Flux.error(() -> new Exception("test")))
		            .verifyErrorMessage("test");
	}

	@Test
	public void throwOnCall() {
		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> new FluxErrorSupplied<>(() -> new IllegalStateException("boom"))
						.call()
				)
				.withMessage("boom");
	}

	@Test
	public void lazilyEvaluatedSubscribe() {
		AtomicInteger count = new AtomicInteger();
		Flux<Object> error = Flux.error(() -> new IllegalStateException("boom" + count.incrementAndGet()));

		assertThat(count).as("no op before subscribe").hasValue(0);

		StepVerifier.create(error.retry(3))
		            .verifyErrorMessage("boom4");
	}

	@Test
	public void lazilyEvaluatedCall() {
		AtomicInteger count = new AtomicInteger();
		FluxErrorSupplied<Object> error = new FluxErrorSupplied<>(() -> new IllegalStateException("boom" + count.incrementAndGet()));

		assertThat(count).as("no op before call").hasValue(0);

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(error::call)
				.withMessage("boom1");

		assertThat(count).as("after call").hasValue(1);
	}

	@Test
	public void supplierMethod() {
		StepVerifier.create(Flux.error(illegalStateExceptionSupplier()))
				.verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(IllegalStateException.class)
						.hasMessage("boom"));
	}

	private Supplier<IllegalStateException> illegalStateExceptionSupplier() {
		return () -> new IllegalStateException("boom");
	}

	@Test
	public void scanOperator(){
	    FluxErrorSupplied<?> test = new FluxErrorSupplied<>(() -> new IllegalStateException());

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
