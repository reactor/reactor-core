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

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * An operator that just bears a name or a set of tags, which can be retrieved via the
 * {@link Attr#TAGS TAGS}
 * attribute.
 *
 * @author Stephane Maldini
 */
final class MonoName<T> extends InternalMonoOperator<T, T> {

	final String name;

	final Set<Tuple2<String, String>> tags;

	@SuppressWarnings("unchecked")
	static <T> Mono<T> createOrAppend(Mono<T> source, String name) {
		Objects.requireNonNull(name, "name");

		if (source instanceof MonoName) {
			MonoName<T> s = (MonoName<T>) source;
			return new MonoName<>(s.source, name, s.tags);
		}
		if (source instanceof MonoNameFuseable) {
			MonoNameFuseable<T> s = (MonoNameFuseable<T>) source;
			return new MonoNameFuseable<>(s.source, name, s.tags);
		}
		if (source instanceof Fuseable) {
			return new MonoNameFuseable<>(source, name, null);
		}
		return new MonoName<>(source, name, null);
	}

	@SuppressWarnings("unchecked")
	static <T> Mono<T> createOrAppend(Mono<T> source, String tagName, String tagValue) {
		Objects.requireNonNull(tagName, "tagName");
		Objects.requireNonNull(tagValue, "tagValue");

		Set<Tuple2<String, String>> tags = Collections.singleton(Tuples.of(tagName, tagValue));

		if (source instanceof MonoName) {
			MonoName<T> s = (MonoName<T>) source;
			if(s.tags != null) {
				tags = new HashSet<>(tags);
				tags.addAll(s.tags);
			}
			return new MonoName<>(s.source, s.name, tags);
		}
		if (source instanceof MonoNameFuseable) {
			MonoNameFuseable<T> s = (MonoNameFuseable<T>) source;
			if (s.tags != null) {
				tags = new HashSet<>(tags);
				tags.addAll(s.tags);
			}
			return new MonoNameFuseable<>(s.source, s.name, tags);
		}
		if (source instanceof Fuseable) {
			return new MonoNameFuseable<>(source, null, tags);
		}
		return new MonoName<>(source, null, tags);
	}

	MonoName(Mono<? extends T> source,
			@Nullable String name,
			@Nullable Set<Tuple2<String, String>> tags) {
		super(source);
		this.name = name;
		this.tags = tags;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return actual;
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.NAME) {
			return name;
		}

		if (key == Attr.TAGS && tags != null) {
			return tags.stream();
		}

		if (key == Attr.RUN_STYLE) {
		    return Attr.RunStyle.SYNC;
		}

		return super.scanUnsafe(key);
	}


}
