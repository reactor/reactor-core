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
package reactor.core.scheduler;

import java.util.concurrent.*;

import org.junit.*;

import reactor.core.scheduler.Scheduler.Worker;

public class ProcessorsSpecTest {

    @Test
    public void ringBufferDispatcher() throws Exception {
        Scheduler serviceRB = Schedulers.newSingle("rb", true);
        Worker dispatcher = serviceRB.createWorker();
        
        try {
            Thread t1 = Thread.currentThread();
            Thread[] t2 = { null };

            CountDownLatch cdl = new CountDownLatch(1);
            
            dispatcher.schedule(() -> { t2[0] = Thread.currentThread(); cdl.countDown(); });
            
            if (!cdl.await(5, TimeUnit.MILLISECONDS)) {
                Assert.fail("ringBufferDispatcher timed out");
            }
            
            Assert.assertNotEquals(t1, t2[0]);
        } finally {
            dispatcher.dispose();
        }
    }
}
