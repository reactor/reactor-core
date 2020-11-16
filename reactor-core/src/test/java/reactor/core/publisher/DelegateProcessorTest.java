/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

// This is ok as this class tests the deprecated DelegateProcessor. Will be removed with it in 3.5.
@SuppressWarnings("deprecation")
public class DelegateProcessorTest {

	@Test
	public void scanReturnsDownStreamForParentElseDelegates() {
		Publisher<?> downstream = Mockito.mock(FluxOperator.class);

		IllegalStateException boom = new IllegalStateException("boom");
		InnerConsumer<?> upstream = Mockito.mock(InnerConsumer.class);
		when(upstream.scanUnsafe(Scannable.Attr.ERROR))
				.thenReturn(boom);
		when(upstream.scanUnsafe(Scannable.Attr.DELAY_ERROR))
				.thenReturn(true);

		DelegateProcessor<?, ?> processor = new DelegateProcessor<>(
				downstream, upstream);

		assertThat(processor.scan(Scannable.Attr.PARENT)).isSameAs(downstream);

		assertThat(processor.scan(Scannable.Attr.ERROR)).isSameAs(boom);
		assertThat(processor.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
	}
}
