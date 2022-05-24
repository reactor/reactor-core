/*
 * Copyright (c) 2011-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
 * Provide main Reactive APIs in {@link reactor.core.publisher.Flux} and {@link reactor.core.publisher.Mono},
 * as well as various helper classes, interfaces used in the composition API, variants of Flux and operator-building
 * utilities.
 *
 * <h2>Flux</h2>
 * A typed N-elements or zero sequence {@link org.reactivestreams.Publisher} with core reactive extensions.
 *
 * <h2>Mono</h2>
 * A typed one-element at most sequence {@link org.reactivestreams.Publisher} with core reactive extensions.
 **
 * @author Stephane Maldini
 */
@NonNullApi
package reactor.core.publisher;

import reactor.util.annotation.NonNullApi;