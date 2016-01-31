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
 * <h2>Mono</h2>
 * <h2>Processors</h2>
 * Reactor offers a few management API via the subclassed {@link reactor.core.publisher.ExecutorProcessor} for the underlying {@link
 * java.util.concurrent.Executor} in use, in addition to the state accessors like
 * {@link reactor.core.state.Backpressurable}.
 * <p>
 * The following {@link org.reactivestreams.Processor} are available:
 * <ul>
 *         <li>It is a synchronous/non-opinionated pub-sub replaying event emitter :
 *           {@link reactor.core.publisher.EmitterProcessor} and {@link reactor.core.publisher.EmitterProcessor#replay}</li>
 *          </li>
 *         <li>A dedicated pub-sub event buffering executor : {@link reactor.core.publisher.TopicProcessor}</li>
 *         <li>A dedicated  FIFO work queue distribution for slow consumers :
 *         {@link reactor.core.publisher.WorkQueueProcessor}</li>
 * </ul>
 * <p>
 *
 * @author Stephane Maldini
 */
package reactor.core.publisher;