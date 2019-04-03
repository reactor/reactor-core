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

import java.util.NoSuchElementException;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class FluxSchedulerCapture<T> extends FluxOperator<T, T> {

	@Nullable
	final Scheduler scheduler;

	FluxSchedulerCapture(Flux<? extends T> source) {
		super(source);
		this.scheduler = Scannable.from(this)
		                          .parents()
		                          .map(parent -> parent.scan(Attr.RUN_ON))
		                          .filter(s -> s instanceof Scheduler)
		                          .map(s -> (Scheduler) s)
		                          .findFirst()
		                          .orElse(null);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;

		return super.scanUnsafe(key);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		if (scheduler != null) {
			Context c = actual.currentContext();
			try {
				FluxPublishOnCaptured.PublishOnCapturedSubscriber<?> schedulerStore = c.get(FluxPublishOnCaptured.PublishOnCapturedSubscriber.class);
				schedulerStore.set(scheduler);
			}
			catch (NoSuchElementException noRunOnCaptured) {
				//NO OP
				//TODO should we fail the operator in this case?
			}
		}
		source.subscribe(actual);
	}
}
