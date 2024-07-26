/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import java.util.function.Supplier;

import static reactor.core.scheduler.Schedulers.BOUNDED_ELASTIC;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE;
import static reactor.core.scheduler.Schedulers.LOOM_BOUNDED_ELASTIC;
import static reactor.core.scheduler.Schedulers.newBoundedElastic;
import static reactor.core.scheduler.Schedulers.factory;

/**
 * JDK 21+ Specific implementation of BoundedElasticScheduler supplier which uses
 * {@link java.lang.ThreadBuilders.VirtualThreadFactory} instead of the default
 * {@link ReactorThreadFactory} when one enables virtual thread support
 */
class BoundedElasticSchedulerSupplier implements Supplier<Scheduler> {

	@Override
	public Scheduler get() {
		if (DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS) {
			Scheduler scheduler = factory.newThreadPerTaskBoundedElastic(
					DEFAULT_BOUNDED_ELASTIC_SIZE,
					DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
					Thread.ofVirtual()
					      .name(LOOM_BOUNDED_ELASTIC + "-", 1)
					      .uncaughtExceptionHandler(Schedulers::defaultUncaughtException)
					      .factory());
			scheduler.init();
			return scheduler;
		}
		return newBoundedElastic(DEFAULT_BOUNDED_ELASTIC_SIZE,
				DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
				BOUNDED_ELASTIC,
				BoundedElasticScheduler.DEFAULT_TTL_SECONDS,
				true);
	}
}
