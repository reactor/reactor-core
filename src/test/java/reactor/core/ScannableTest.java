/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class ScannableTest {

	static final Scannable scannable = key -> {
		switch (key) {
			case BUFFERED:
				return 1;
			case TERMINATED:
				return true;
			case PARENT:
				return null;
			case ACTUAL:
				return (Scannable) k -> (Scannable) k2 -> null;
		}
		return null;
	};

	@Test
	public void unavailableScan() {
		assertThat(Scannable.from("nothing")).isEqualTo(Scannable.Attr.UNAVAILABLE_SCAN);
		assertThat(Scannable.from("nothing").isScanAvailable()).isFalse();
		assertThat(Scannable.from("nothing").inners().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").parents().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").actuals().count()).isEqualTo(0);
		assertThat(Scannable.from("nothing").scan(Scannable.Attr.TERMINATED, Boolean.class)).isNull();
		assertThat(Scannable.from("nothing").scanOrDefault(Scannable.Attr.BUFFERED, 0)).isEqualTo(0);
		assertThat(Scannable.from("nothing").scan(Scannable.Attr.ACTUAL)).isNull();
	}

	@Test
	public void availableScan() {
		assertThat(Scannable.from(scannable)).isEqualTo(scannable);
		assertThat(Scannable.from(scannable).isScanAvailable()).isTrue();
		assertThat(Scannable.from(scannable).inners().count()).isEqualTo(0);
		assertThat(Scannable.from(scannable).parents().count()).isEqualTo(0);
		assertThat(Scannable.from(scannable).actuals().count()).isEqualTo(2);
		assertThat(Scannable.from(scannable).scan(Scannable.Attr.TERMINATED, Boolean.class)).isTrue();
		assertThat(Scannable.from(scannable).scanOrDefault(Scannable.Attr.BUFFERED, 0)).isEqualTo(1);
		assertThat(Scannable.from(scannable).scan(Scannable.Attr.ACTUAL)).isEqualTo(scannable.actuals().findFirst().get());
	}

	@Test
	public void nullScan() {
		assertThat(Scannable.from(null)).isNull();
	}
}
