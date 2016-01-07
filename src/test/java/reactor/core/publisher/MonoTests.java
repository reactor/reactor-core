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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.Mono;
import reactor.Processors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Stephane Maldini
 */
public class MonoTests {



	// test issue https://github.com/reactor/reactor/issues/485
	@Test
	public void promiseOnErrorHandlesExceptions() throws Exception {
		CountDownLatch latch1 = new CountDownLatch(1);
		CountDownLatch latch2 = new CountDownLatch(1);

		try {
			Mono.fromCallable(() -> {
				throw new RuntimeException("Some Exception");
			})
			    .publishOn(Processors.ioGroup())
			    .doOnError(t -> latch1.countDown())
			    .doOnSuccess(v -> latch2.countDown())
			    .get();
		}
		catch (RuntimeException re){

		}
		assertThat("Error latch was counted down", latch1.await(1, TimeUnit.SECONDS), is(true));
		assertThat("Complete latch was not counted down", latch2.getCount(), is(1L));
	}

	@Test
	public void promiseOnAfterHandlesExceptions() throws Exception {
		String h = Mono.fromCallable(() -> {
			Thread.sleep(400);
			return "hello";
		})
		               .publishOn(Processors.ioGroup())
		               .after(() -> Mono.just("world"))
		               .get();
		assertThat("Alternate mono not seen", h, is("world"));
	}
}
