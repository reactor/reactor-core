/*
 * Copyright (c) 2011-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
 * The following
 * {@link org.reactivestreams.Processor} extending {@link reactor.core.publisher.FluxProcessor} are available:
 * <ul>
 *         <li>A synchronous/non-opinionated pub-sub replaying capable event emitter :
 *         {@link reactor.core.publisher.EmitterProcessor},
 *         {@link reactor.core.publisher.ReplayProcessor},
 *         {@link reactor.core.publisher.UnicastProcessor} and
 *         {@link reactor.core.publisher.DirectProcessor}</li>
 *         <li>{@link reactor.core.publisher.FluxProcessor} itself offers factories to build arbitrary {@link org.reactivestreams.Processor}</li>
 * </ul>
 * <p>
 **
 * @author Stephane Maldini
 */
@NonNullApi
package reactor.core.publisher;

import reactor.util.annotation.NonNullApi;