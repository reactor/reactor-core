/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

/**
 * An operator that just bears a set of tags, which can be retrieved via the {@link reactor.core.Scannable.GenericAttr#TAGS TAGS}
 * attribute.
 *
 * @author Simon Baslé
 */
class FluxTagged<T> extends FluxOperator<T, T> {

	final Set<String> tags;

	FluxTagged(Flux<? extends T> source, String... tags) {
		super(source);
		this.tags = new HashSet<>();
		this.tags.addAll(Arrays.asList(tags));
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(new TagsSubscriber<>(actual, this.tags));
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == GenericAttr.TAGS) return tags;

		return super.scanUnsafe(key);
	}

	static final class TagsSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final Set<String> tags;

		Subscription s;

		TagsSubscriber(CoreSubscriber<? super T> actual, Set<String> tags) {
			this.actual = actual;
			this.tags = tags;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if(Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == GenericAttr.TAGS) return tags;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}
	}
}
