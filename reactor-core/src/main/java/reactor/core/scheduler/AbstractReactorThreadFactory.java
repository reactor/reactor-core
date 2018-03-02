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

	final protected String  name;
	final private   boolean rejectBlocking;

	public AbstractReactorThreadFactory(String name) {
		this.name = name;
		this.rejectBlocking = false;
	}

	public AbstractReactorThreadFactory(String name, boolean rejectBlocking) {
		this.name = name;
		this.rejectBlocking = rejectBlocking;
	}

	@Override
	public final Thread newThread(@NotNull Runnable runnable) {
		Thread t = rejectBlocking
				? new NonBlockingThread(runnable, newThreadName())
				: new Thread(runnable, newThreadName());
		configureThread(t);
		return t;
	}

	protected abstract String newThreadName();

	protected void configureThread(Thread t) {
		//NO-OP by default
	}

	/**
	 * Get the prefix used for new {@link Thread Threads} created by this {@link ThreadFactory}.
	 * The factory can also be seen as a {@link Supplier Supplier&lt;String&gt;}.
	 *
	 * @return the thread name prefix
	 */
	@Override
	public String get() {
		return name;
	}

	/**
	 * @return true if produced {@link Thread} are marked as {@link NonBlocking}-only.
	 */
	public boolean isBlockingRejected() {
		return rejectBlocking;
	}

	static final class NonBlockingThread extends Thread implements NonBlocking {

		public NonBlockingThread(Runnable target, String name) {
			super(target, name);
		}
	}
}
