/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

/**
 * Provide for
 * {@link reactor.core.publisher.Flux}, {@link reactor.core.publisher.Mono} composition API and {@link reactor.core.publisher.FluxProcessor}
 * implementations
 *
 * <h2>Flux</h2>
 * A typed N-elements or zero sequence {@link org.reactivestreams.Publisher} with core reactive extensions.
 *
 * <h2>Mono</h2>
 * A typed one-element at most sequence {@link org.reactivestreams.Publisher} with core reactive extensions.
 *
 * <h2>Processors</h2>
 * The following
 * {@link org.reactivestreams.Processor} extending {@link reactor.core.publisher.FluxProcessor} are available:
 * <ul>
 *         <li>A synchronous/non-opinionated pub-sub replaying capable event emitter :
 *         {@link reactor.core.publisher.EmitterProcessor} and {@link reactor.core.publisher.EmitterProcessor#replay}</li>
 *          </li>
 *         <li>A dedicated parallel pub-sub event buffering broadcaster :
 *         {@link reactor.core.publisher.TopicProcessor}</li>
 *         <li>A dedicated parallel work queue distribution for slow consumers :
 *         {@link reactor.core.publisher.WorkQueueProcessor}</li>
 *         <li>{@link reactor.core.publisher.FluxProcessor} itself offers factories to build arbitrary {@link org.reactivestreams.Processor}</li>
 * </ul>
 * Reactor offers a few management API via the subclassed {@link reactor.core.publisher.EventLoopProcessor} for the underlying {@link
 * java.util.concurrent.Executor} in use, in addition to the state accessors like
 * {@link reactor.core.state.Backpressurable}.
 * <p>
 ** <h2>Schedulers</h2>
 * Scheduling in Reactor is a couple of general concepts shared by reactive operators
 * "{@link reactor.core.publisher.Flux#publishOn publishOn}" and
 * "{@link reactor.core.publisher.Flux#subscribeOn subscribeOn}".
 * <ul>
 *     <li>Scheduler: a
 *     {@link java.util.function.Consumer} of {@link java.lang.Runnable} accepting tasks to run or {@literal null} as a
 *     terminal signal used for cleanup logic</li>
 *     <li>Scheduler Factory: a {@link java.util.concurrent.Callable} generating schedulers. A major implementation
 *     of it is {@link reactor.core.publisher.SchedulerGroup}, a reference-counting worker generator that release
 *     resources automatically after worker terminaisons.
 *     </li>
 * </ul>
 * The key difference between asynchronous processors and schedulers is their natural ability to be shared where
 * {@link org.reactivestreams.Processor} are bound to a single logical producer by {@link org.reactivestreams.Subscription}.
 * Thus, when no dedicated threading is required (hot or critical data pipeline), it is recommended to consider
 * schedulers + publishOn/subscribeOn over processors.
 * @author Stephane Maldini
 */
package reactor.core.publisher;