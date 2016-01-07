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
package reactor.core.publisher;

import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.SkipException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import reactor.Flux;
import reactor.Timers;
import reactor.core.publisher.convert.DependencyUtils;

/**
 * @author Stephane Maldini
 */
@Test
public class Jdk9PublisherTests extends PublisherVerification<Long> {

	public Jdk9PublisherTests() {
		super(new TestEnvironment(500, true), 1000);
	}

	@BeforeTest
	public void before(){
		Timers.global();
	}

	@org.junit.Test
	public void simpleTest(){

	}

	@Override
	public long maxElementsFromPublisher() {
		return 1;
	}

	@Override
	public Publisher<Long> createPublisher(long elements) {
		if(!DependencyUtils.hasJdk9Flow()){
			throw new SkipException("no jdk 9 classes found");
		}

		SubmissionPublisher<Long> pub = new SubmissionPublisher<>();
		Timers.global().schedule(pub::submit, 50, TimeUnit.MILLISECONDS);
		return Flux.convert(pub);
	}

	@Override
	public Publisher<Long> createFailedPublisher() {
		if(!DependencyUtils.hasJdk9Flow()){
			throw new SkipException("no jdk 9 classes found");
		}

		SubmissionPublisher<Long> pub = new SubmissionPublisher<>();
		pub.closeExceptionally(new Exception("jdk9-test"));
		return Flux.convert(pub);
	}
}
