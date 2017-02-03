/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.Test;
import reactor.core.Receiver;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSupplierTest {

	@Test
	public void normal() {
		AtomicInteger n = new AtomicInteger();
		Mono<Integer> m = Mono.fromSupplier(n::incrementAndGet);

		m.subscribeWith(AssertSubscriber.create())
				.assertValues(1)
				.assertComplete();

		m.subscribeWith(AssertSubscriber.create())
				.assertValues(2)
				.assertComplete();
	}

	@Test
	public void supplierCancel(){
		StepVerifier.create(Mono.fromSupplier(() -> "test"))
	                .thenCancel()
	                .verify();
	}

	@Test
	public void supplierThrows() {
		StepVerifier.create(Mono.fromSupplier(() -> {
			throw new RuntimeException("forced failure");
		}))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void supplierUpstream() {
		Supplier<?> s = () -> "test";

		assertThat(((Receiver)Mono.fromSupplier(s)).upstream()).isEqualTo(s);
	}

	@Test
	public void onMonoSuccessSupplierOnBlock() {
		assertThat(Mono.fromSupplier(() -> "test")
		               .block()).isEqualToIgnoringCase("test");
	}

	@Test(expected = RuntimeException.class)
	public void onMonoErrorSupplierOnBlock() {
		Mono.fromSupplier(() -> {
			throw new RuntimeException("test");
		})
		    .block();
	}
}