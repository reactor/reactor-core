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

/**
 * Provide for
 * {@link reactor.core.publisher.Flux}, {@link reactor.core.publisher.Mono} composition
 * API and {@link org.reactivestreams.Processor} implementations
 *
 * <h2>Flux</h2>
 * A typed N-elements or zero sequence {@link org.reactivestreams.Publisher} with core reactive extensions.
 *
 * <h2>Mono</h2>
 * A typed one-element at most sequence {@link org.reactivestreams.Publisher} with core reactive extensions.
 *
 * <h2>Processors</h2>
 * The following {@link reactor.core.publisher.FluxProcessorSink} are available, which can
 * be converted to {@link org.reactivestreams.Processor}:
 * <ul>
 *         <li>Synchronous/non-opinionated pub-sub replaying capable event emitter :
 *         {@link reactor.core.publisher.Processors#emitter()},
 *         {@link reactor.core.publisher.Processors#replay()},
 *         {@link reactor.core.publisher.Processors#unicast()} and
 *         {@link reactor.core.publisher.Processors#direct()}</li>
 *         <li>A dedicated parallel pub-sub event buffering broadcaster :
 *         {@link reactor.core.publisher.Processors#fanOut()}</li>
 *         <li>A dedicated parallel work queue distribution for slow consumers :
 *         {@link reactor.core.publisher.Processors#relaxedFanOut()}</li>
 * </ul>
 * <p>
 **
 * @author Stephane Maldini
 */
@NonNullApi
package reactor.core.publisher;

import reactor.util.annotation.NonNullApi;