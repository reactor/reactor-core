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

package reactor.core.publisher.convert;

import org.reactivestreams.Publisher;
import reactor.Mono;
import rx.Observable;
import rx.Single;

/**
 * @author Stephane Maldini
 */
public final class RxJava1SingleConverter extends PublisherConverter<Single> {

	static final RxJava1SingleConverter INSTANCE = new RxJava1SingleConverter();

	@SuppressWarnings("unchecked")
	static public <T> Single<T> from(Publisher<T> o) {
		return INSTANCE.fromPublisher(o);
	}

	@SuppressWarnings("unchecked")
	static public <T> Publisher<T> from(Single<T> o) {
		return INSTANCE.toPublisher(o);
	}

	@Override
	public Single fromPublisher(Publisher<?> o) {
		Observable obs = DependencyUtils.convertFromPublisher(o, Observable.class);
		if (obs != null) {
			return obs.toSingle();
		}
		return null;
	}

	@Override
	public Mono toPublisher(Object o) {
		Single<?> single = (Single<?>) o;
		return Mono.from(DependencyUtils.convertToPublisher(single.toObservable()));
	}

	@Override
	public Class<Single> get() {
		return Single.class;
	}
}
