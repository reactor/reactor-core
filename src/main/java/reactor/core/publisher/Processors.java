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

package reactor.core.publisher;

import org.reactivestreams.Processor;

/**
 * Main gateway to build various reactive {@link Processor} or "pooled" groups that allow their reuse.
 * Reactor offers a few management API via the subclassed {@link ExecutorProcessor} for the underlying {@link
 * java.util.concurrent.Executor} in use, in addition to the state accessors like
 * {@link reactor.core.state.Backpressurable}.
 * <p>
 * There are 2+2 decisions to make when choosing a factory from {@link Processors} :
 * <ul>
 *     <li>It is supporting a dynamically created data flow ({@link Flux} or {@link Mono}): <ul>
 *         <li>It is a synchronous/non-opinionated pub-sub replaying event emitter :
 *     {@link EmitterProcessor#create} EmitterProcessor} and {@link EmitterProcessor#replay}</li>
 *         <li>It needs asynchronousity :
 *         <ul>
 *           <li>for slow publishers prefer {@link ProcessorGroup#io()} and {@link ProcessorGroup#publishOn}</li>
 *           <li>for fast publisher prefer {@link ProcessorGroup#async} and {@link ProcessorGroup#dispatchOn}</li>
 *         </ul>
 *        </li>
 *     </ul></li>
 *     <li>It is a demanding data flow : <ul>
 *         <li>A dedicated pub-sub event buffering executor : {@link TopicProcessor#create}</li>
 *         <li>A dedicated  FIFO work queue distribution for slow consumers : {@link WorkQueueProcessor#create}</li>
 *     </ul></li>
 * </ul>
 * <p>
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public enum Processors {

}
