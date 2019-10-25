/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor.test;

import org.junit.rules.ExternalResource;

import reactor.core.Disposable;
import reactor.core.Disposables;

/**
 * A JUnit {@link org.junit.Rule} that exposes a way to automatically cleanup arbitrary
 * {@link Disposable} instances after a test, in a fluent fashion.
 *
 * @author Simon Baslé
 */
public class AutoDisposingRule extends ExternalResource {

	private Disposable.Composite toDispose;

	@Override
	protected void before() throws Throwable {
		toDispose = Disposables.composite();
	}

	@Override
	protected void after() {
		toDispose.dispose();
	}

	/**
	 * Register a {@link Disposable} for automatic cleanup and return it for chaining.
	 * @param disposable the resource to clean up at end of test
	 * @param <T> the type of the resource
	 * @return the resource
	 */
	public <T extends Disposable> T autoDispose(T disposable) {
		toDispose.add(disposable);
		return disposable;
	}
}
