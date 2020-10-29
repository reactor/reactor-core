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
import reactor.core.Fuseable;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoHideTest {

	@Test
	public void normal() {
		Mono<Integer> f = Mono.just(1);
		assertThat(f).isInstanceOf(Fuseable.ScalarCallable.class);
		f = f.hide();
		assertThat(f).isNotInstanceOf(Fuseable.ScalarCallable.class);
		assertThat(f).isInstanceOf(MonoHide.class);
	}

	@Test
	public void scanOperator(){
		Mono<Integer> parent = Mono.just(1);
		MonoHide<Integer> test = new MonoHide<>(parent);

	    assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
	    assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
