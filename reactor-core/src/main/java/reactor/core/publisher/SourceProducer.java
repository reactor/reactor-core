/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 *
 * {@link SourceProducer} is a {@link Publisher} that is {@link Scannable} for the
 * purpose of being tied back to a chain of operators. By itself it doesn't allow
 * walking the hierarchy of operators, as they can only be tied from downstream to upstream
 * by referencing their sources.
 *
 * @param <O> output operator produced type
 *
 * @author Simon Baslé
 */
interface SourceProducer<O> extends Scannable, Publisher<O> {

	@Override
	@Nullable
	default Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return Scannable.from(null);
		if (key == Attr.ACTUAL) return Scannable.from(null);

		return null;
	}

	@Override
	default String stepName() {
		return "source(" + getClass().getSimpleName() + ")";
	}
}
