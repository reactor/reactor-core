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
 * Common traits shared by reactive components: backpressure, activity state, request tracking, or connected
 * upstreams/downstreams.
 * <p>
 * The state read accuracy (volatility) is implementation-dependent and implementors MAY return cached value for a given
 * state.
 *
 * @author Stephane Maldini
 */

package reactor.core.trait;