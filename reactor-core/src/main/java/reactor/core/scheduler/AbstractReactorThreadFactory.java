/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;

/**
 * A base class for {@link ThreadFactory Thread factories} to be used by {@link Scheduler},
 * creating {@link Thread} with a prefix (which can be retrieved with the {@link #get()} method).
 * @author Simon Baslé
 */
public abstract class AbstractReactorThreadFactory
		implements ThreadFactory, Supplier<String> {

	final protected String     name;

	public AbstractReactorThreadFactory(String name) {
		this.name = name;
	}

	@Override
	public final Thread newThread(@NotNull Runnable runnable) {
		Thread t = new Thread(runnable, newThreadName());
		configureThread(t);
		return t;
	}

	protected String newThreadName() {
		return name;
	}

	protected void configureThread(Thread t) {
		//NO-OP by default
	}

	@Override
	public String get() {
		return name;
	}
}
