/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Test;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxPublishOnCapturedTest {

	@Test
	public void smokeTest() {
		Scheduler expected = Schedulers.newSingle("fluxPublishOnCapturedTest");
		try {
			Flux<String> opaqueSource = Flux.just(1, 2, 3)
			                                .publishOn(expected)
			                                .map(i -> i + " on " + Thread.currentThread().getName());

			final Flux<String> flux = opaqueSource.captureScheduler()
			                                      .concatMap(s -> Mono.fromCallable(() -> s + " flatMap on " + Thread.currentThread().getName())
			                                                        .subscribeOn(Schedulers.elastic()))
			                                      .publishOn(Schedulers.parallel())
			                                      .map(s -> s + " published on " + Thread.currentThread().getName())
			                                      .publishOnCapturedOr(Schedulers::immediate)
			                                      .map(s -> s + " returned on " + Thread.currentThread().getName());

			StepVerifier.create(flux)
			            .assertNext(s -> assertThat(s).matches("1 on (fluxPublishOnCapturedTest-[0-9]*) flatMap on elastic-[0-9]* published on parallel-[0-9]* returned on \\1"))
			            .assertNext(s -> assertThat(s).matches("2 on (fluxPublishOnCapturedTest-[0-9]*) flatMap on elastic-[0-9]* published on parallel-[0-9]* returned on \\1"))
			            .assertNext(s -> assertThat(s).matches("3 on (fluxPublishOnCapturedTest-[0-9]*) flatMap on elastic-[0-9]* published on parallel-[0-9]* returned on \\1"))
			            .verifyComplete();
		}
		finally {
			expected.dispose();
		}
	}

	@Test
	public void smokeTestNoReturn() {
		Scheduler expected = Schedulers.newSingle("fluxPublishOnCapturedTest");
		try {
			Flux<String> opaqueSource = Flux.just(1, 2, 3)
			                                .publishOn(expected)
			                                .map(i -> i + " on " + Thread.currentThread().getName());

			final Flux<String> flux = opaqueSource.captureScheduler()
			                                      .concatMap(s -> Mono.fromCallable(() -> s + " flatMap on " + Thread.currentThread().getName())
			                                                        .subscribeOn(Schedulers.elastic()))
			                                      .publishOn(Schedulers.parallel())
			                                      .map(s -> s + " published on " + Thread.currentThread().getName());

			StepVerifier.create(flux)
			            .assertNext(s -> assertThat(s).matches("1 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on parallel-[0-9]*"))
			            .assertNext(s -> assertThat(s).matches("2 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on parallel-[0-9]*"))
			            .assertNext(s -> assertThat(s).matches("3 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on parallel-[0-9]*"))
			            .verifyComplete();
		}
		finally {
			expected.dispose();
		}
	}

	@Test
	public void smokeTestNoCaptureOrImmediate() {
		Scheduler expected = Schedulers.newSingle("fluxPublishOnCapturedTest");
		try {
			Flux<String> opaqueSource = Flux.just(1, 2, 3)
			                                .publishOn(expected)
			                                .map(i -> i + " on " + Thread.currentThread().getName());

			final Flux<String> flux = opaqueSource.concatMap(s -> Mono.fromCallable(() -> s + " flatMap on " + Thread.currentThread().getName())
			                                                        .subscribeOn(Schedulers.elastic()))
			                                      .publishOn(Schedulers.parallel())
			                                      .map(s -> s + " published on " + Thread.currentThread().getName())
			                                      .publishOnCapturedOr(Schedulers::immediate)
			                                      .map(s -> s + " returned on " + Thread.currentThread().getName());

			StepVerifier.create(flux)
			            .assertNext(s -> assertThat(s).matches("1 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on (parallel-[0-9]*) returned on \\1"))
			            .assertNext(s -> assertThat(s).matches("2 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on (parallel-[0-9]*) returned on \\1"))
			            .assertNext(s -> assertThat(s).matches("3 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on (parallel-[0-9]*) returned on \\1"))
			            .verifyComplete();
		}
		finally {
			expected.dispose();
		}
	}

	@Test
	public void smokeTestNoCaptureOrSpecific() {
		Scheduler expected = Schedulers.newSingle("fluxPublishOnCapturedTest");
		Scheduler defaultReturn = Schedulers.newSingle("fluxPublishOnCapturedTestReturn");
		try {
			Flux<String> opaqueSource = Flux.just(1, 2, 3)
			                                .publishOn(expected)
			                                .map(i -> i + " on " + Thread.currentThread().getName());

			final Flux<String> flux = opaqueSource.concatMap(s -> Mono.fromCallable(() -> s + " flatMap on " + Thread.currentThread().getName())
			                                                        .subscribeOn(Schedulers.elastic()))
			                                      .publishOn(Schedulers.parallel())
			                                      .map(s -> s + " published on " + Thread.currentThread().getName())
			                                      .publishOnCapturedOr(() -> defaultReturn)
			                                      .map(s -> s + " returned on " + Thread.currentThread().getName());

			StepVerifier.create(flux)
			            .assertNext(s -> assertThat(s).matches("1 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on parallel-[0-9]* returned on fluxPublishOnCapturedTestReturn-[0-9]*"))
			            .assertNext(s -> assertThat(s).matches("2 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on parallel-[0-9]* returned on fluxPublishOnCapturedTestReturn-[0-9]*"))
			            .assertNext(s -> assertThat(s).matches("3 on fluxPublishOnCapturedTest-[0-9]* flatMap on elastic-[0-9]* published on parallel-[0-9]* returned on fluxPublishOnCapturedTestReturn-[0-9]*"))
			            .verifyComplete();
		}
		finally {
			expected.dispose();
			defaultReturn.dispose();
		}
	}

}