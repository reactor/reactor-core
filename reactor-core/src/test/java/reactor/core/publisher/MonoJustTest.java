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

import java.util.Optional;

import org.junit.jupiter.api.Test;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoJustTest {

    @Test
    public void nullValue() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoJust<Integer>(null);
		});
	}

    @Test
    public void valueSame() {
	    try {
		    assertThat(new MonoJust<>(1).call()).isEqualTo(1);
	    }
	    catch (Exception e) {
		    e.printStackTrace();
	    }
    }

    @Test
    public void normal() {
	    StepVerifier.create(Mono.just(1))
	                .expectNext(1)
	                .verifyComplete();
    }

    @Test
    public void normalOptional() {
	    StepVerifier.create(Mono.justOrEmpty(Optional.of(1)))
	                .expectNext(1)
	                .verifyComplete();
    }

	@Test
	public void normalOptionalOfNullable() {
		StepVerifier.create(Mono.justOrEmpty(Optional.ofNullable(null)))
				.verifyComplete();
	}

	@Test
	public void normalScalarOptionalEmpty() {
		StepVerifier.create(Mono.justOrEmpty(null))
		            .verifyComplete();
	}


	@Test
	public void normalScalarOptionalEmpty2() {
		StepVerifier.create(Mono.justOrEmpty((Object)null))
		            .verifyComplete();
	}

	@Test
	public void normalScalarOptional() {
		StepVerifier.create(Mono.justOrEmpty(1))
		            .expectNext(1)
		            .verifyComplete();
	}

    @Test
    public void normalOptionalEmpty() {
        StepVerifier.create(Mono.justOrEmpty(Optional.empty()))
                    .verifyComplete();
    }

    @Test
    public void normalBackpressured() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

        Mono.just(1).subscribe(ts);

        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();

        ts.request(1);

        ts.assertValues(1)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void fused() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Mono.just(1).subscribe(ts);
        
        ts.assertFuseableSource()
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1);
    }

	@Test
	public void onMonoSuccessReturnOnBlock() {
		assertThat(Mono.just("test")
		               .block()).isEqualToIgnoringCase("test");
	}

	@Test
	public void scanOperator() {
    	MonoJust s = new MonoJust<>("foo");

    	assertThat(s.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
    	assertThat(s.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

}
