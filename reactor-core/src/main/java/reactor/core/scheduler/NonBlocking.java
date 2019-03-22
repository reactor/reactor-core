/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.scheduler;

import java.util.concurrent.ThreadFactory;

/**
 * A marker interface that is detected on {@link Thread Threads} while executing Reactor
 * blocking APIs, resulting in these calls throwing an exception.
 * <p>
 * Extend {@link AbstractReactorThreadFactory} for a {@link ThreadFactory} that can easily
 * create such threads, and optionally name them and further configure them.
 * <p>
 * See {@link Schedulers#isBlockingCurrentThreadOk()} and
 * {@link Schedulers#isBlockingCurrentThreadOk(Thread)} for a check that includes detecting
 * this marker interface.
 *
 * @author Simon Baslé
 */
public interface NonBlocking { }
