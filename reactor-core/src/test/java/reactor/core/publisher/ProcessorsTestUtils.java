/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import reactor.core.CoreSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Internal utility methods for tests around {@link org.reactivestreams.Processor} and
 * {@link FluxProcessorSink} APIs.
 *
 * @author Simon Baslé
 */
public class ProcessorsTestUtils {

	/**
	 * Method to extract the {@code actual} from a unicast {@link FluxProcessorSink}
	 * @param unicast the unicast processor
	 * @param <T> the type of the processor
	 * @return the actual {@link CoreSubscriber} the unicast processor is attached to
	 */
	public static <T> CoreSubscriber<T> unicastActual(FluxProcessorSink<T> unicast) {
		assertThat(unicast.asProcessor()).isInstanceOf(InnerOperator.class);
		@SuppressWarnings("unchecked")
		CoreSubscriber<T> actual = ((InnerOperator) unicast.asProcessor()).actual();
		return actual;
	}

}
