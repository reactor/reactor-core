/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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

public class ConnectableFluxHideTest {

	@Test
	public void hideApiPreservesConnectableFlux() {
		ConnectableFlux<Integer> connectableFlux = Flux.range(1, 4).replay();

		assertThat(connectableFlux).as("original is Fuseable").isInstanceOf(Fuseable.class);

		//this validates the hide() maintains the API
		connectableFlux = connectableFlux.hide();
		connectableFlux.connect();

		assertThat(connectableFlux).as("hidden is not fuseable").isNotInstanceOf(Fuseable.class);
	}

	@Test
	public void scanOperator() throws Exception {
		ConnectableFlux<Integer> source = Flux.range(1, 4).publish();
		ConnectableFluxHide<Integer> test = new ConnectableFluxHide<>(source);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH))
				.isEqualTo(256)
				.isEqualTo(source.getPrefetch());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
