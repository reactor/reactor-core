/*
 * Copyright (c) 2020-Present VMware Inc. or its affiliates, All Rights Reserved.
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

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import reactor.core.Disposable;
import reactor.core.Disposables;

/**
 * A JUnit {@link Extension} that exposes a way to automatically cleanup arbitrary
 * {@link Disposable} instances after a test, in a fluent fashion.
 *
 * @author Simon Baslé
 * @author Eric Bottard
 */
public class AutoDisposingExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {

	private Disposable.Composite toDispose;

	@Override
	public void beforeTestExecution(ExtensionContext context) throws Exception {
		toDispose = Disposables.composite();
	}

	@Override
	public void afterTestExecution(ExtensionContext context) throws Exception {
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