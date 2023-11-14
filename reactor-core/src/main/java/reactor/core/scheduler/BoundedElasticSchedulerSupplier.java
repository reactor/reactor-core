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

import reactor.util.Logger;
import reactor.util.Loggers;

import static reactor.core.scheduler.Schedulers.BOUNDED_ELASTIC;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE;
import static reactor.core.scheduler.Schedulers.newBoundedElastic;

/**
 * JDK 8 Specific implementation of BoundedElasticScheduler supplier, which warns when
 * one enables virtual thread support. An alternative variant is available for use on JDK 21+
 * where virtual threads are supported.
 */
class BoundedElasticSchedulerSupplier implements Supplier<Scheduler> {

	static final Logger logger = Loggers.getLogger(BoundedElasticSchedulerSupplier.class);

	@Override
	public Scheduler get() {
		if (DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS) {
			logger.warn(
					"Virtual Threads support is not available on the given JVM. Falling back to default BoundedElastic setup");
		}

		return newBoundedElastic(DEFAULT_BOUNDED_ELASTIC_SIZE,
				DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
				BOUNDED_ELASTIC,
				BoundedElasticScheduler.DEFAULT_TTL_SECONDS,
				true);
	}
}
