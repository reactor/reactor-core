/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.scheduler;

import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class ElasticSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.elastic();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void unsupportedStart() throws Exception {
		Schedulers.elastic().start();
	}

	@Override
	protected boolean shouldCheckInterrupted() {
		return true;
	}

	@Test(timeout = 10000)
	public void recycle() throws Exception {
		Scheduler s = Schedulers.newElastic("test-recycle", 1);

		try{
			Disposable d = (Disposable)s.schedule(() -> Flux.never().blockFirst());
			assertThat(d.isDisposed()).isFalse();
			d.dispose();
			while(((ElasticScheduler)s).cache.isEmpty());
			while(!((ElasticScheduler)s).cache.isEmpty());
		}
		finally {
			s.shutdown();
			s.dispose();//noop
		}

		assertThat(((ElasticScheduler)s).cache).isEmpty();
		assertThat(s.isDisposed()).isTrue();
	}
}
