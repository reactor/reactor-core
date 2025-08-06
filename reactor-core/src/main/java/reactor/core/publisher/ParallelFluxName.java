/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

/**
 * Hides the identities of the upstream Publisher object and its Subscription as well.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class ParallelFluxName<T> extends ParallelFlux<T> implements Scannable{

	final ParallelFlux<T> source;

	final String name;

	final List<Tuple2<String, String>> tagsWithDuplicates;

	@SuppressWarnings("unchecked")
	static <T> ParallelFlux<T> createOrAppend(ParallelFlux<T> source, String name) {
		Objects.requireNonNull(name, "name");

		if (source instanceof ParallelFluxName) {
			ParallelFluxName<T> s = (ParallelFluxName<T>) source;
			return new ParallelFluxName<>(s.source, name, s.tagsWithDuplicates);
		}
		return new ParallelFluxName<>(source, name, null);
	}

	@SuppressWarnings("unchecked")
	static <T> ParallelFlux<T> createOrAppend(ParallelFlux<T> source, String tagName, String tagValue) {
		Objects.requireNonNull(tagName, "tagName");
		Objects.requireNonNull(tagValue, "tagValue");

		Tuple2<String, String> newTag = Tuples.of(tagName, tagValue);

		if (source instanceof ParallelFluxName) {
			ParallelFluxName<T> s = (ParallelFluxName<T>) source;
			List<Tuple2<String, String>> tags;
			if(s.tagsWithDuplicates != null) {
				tags = new LinkedList<>(s.tagsWithDuplicates);
				tags.add(newTag);
			}
			else {
				tags = Collections.singletonList(newTag);
			}
			return new ParallelFluxName<>(s.source, s.name, tags);
		}
		return new ParallelFluxName<>(source, null, Collections.singletonList(newTag));
	}

	ParallelFluxName(ParallelFlux<T> source,
			@Nullable String name,
			@Nullable List<Tuple2<String, String>> tags) {
		this.source = ParallelFlux.from(source);
		this.name = name;
		this.tagsWithDuplicates = tags;
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.NAME) {
			return name;
		}

		if (key == Attr.TAGS && tagsWithDuplicates != null) {
			return tagsWithDuplicates.stream();
		}

		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();

		if (key == RUN_STYLE) {
		    return SYNC;
		}

		if (key == InternalProducerAttr.INSTANCE) return true;

		return null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		source.subscribe(subscribers);
	}
}
