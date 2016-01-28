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
 * There are 2+2 decisions to make when choosing a {@link org.reactivestreams.Processor} :
 * <ul>
 *     <li>It is supporting a dynamically created data flow
 *     ({@link reactor.core.publisher.Flux} or {@link reactor.core.publisher.Mono}): <ul>
 *         <li>It is a synchronous/non-opinionated pub-sub replaying event emitter :
 *
 *     {@link reactor.core.publisher.EmitterProcessor#create} EmitterProcessor} and {@link reactor.core.publisher.EmitterProcessor#replay}</li>
 *         <li>It needs asynchronousity :
 *         <ul>
 *           <li>for slow publishers prefer
 *
 *           {@link reactor.core.publisher.ProcessorGroup#io()} and {@link reactor.core.publisher.ProcessorGroup#publishOn}</li>
 *           <li>for fast publisher prefer
 *
 *           {@link reactor.core.publisher.ProcessorGroup#async} and {@link reactor.core.publisher.ProcessorGroup#dispatchOn}</li>
 *         </ul>
 *        </li>
 *     </ul></li>
 *     <li>It is a demanding data flow : <ul>
 *         <li>A dedicated pub-sub event buffering executor : {@link reactor.core.publisher.TopicProcessor#create}</li>
 *         <li>A dedicated  FIFO work queue distribution for slow consumers :
 *         {@link reactor.core.publisher.WorkQueueProcessor#create}</li>
 *     </ul></li>
 * </ul>
 * <p>
 *
 * @author Stephane Maldini
 */
package reactor.core.publisher;