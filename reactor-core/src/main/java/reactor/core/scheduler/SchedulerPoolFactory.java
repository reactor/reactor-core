/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.scheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the creating of ScheduledThreadPoolExecutor and sets up purging
 *
 * @author aftersss
 */
public final class SchedulerPoolFactory {
    private SchedulerPoolFactory() {
        throw new IllegalStateException("No instances!");
    }

    static final String PURGE_ENABLED_KEY = "reactor.purge-enabled";

    /**
     * Indicates the periodic purging of the ScheduledExecutorService is enabled.
     */
    public static final boolean PURGE_ENABLED;

    static final String PURGE_PERIOD_SECONDS_KEY = "reactor.purge-period-seconds";

    /**
     * Indicates the purge period of the ScheduledExecutorServices.
     */
    public static final int PURGE_PERIOD_SECONDS;

    static final AtomicReference<ScheduledExecutorService> PURGE_THREAD = new AtomicReference<>();

    static final Map<ScheduledThreadPoolExecutor, Object> POOLS = new ConcurrentHashMap<>();

    static final AtomicLong COUNTER = new AtomicLong();

    static final ThreadFactory PURGE_THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "reactor-scheduler-pool-purge-" + COUNTER.incrementAndGet());
        t.setDaemon(true);
        return t;
    };

    /**
     * Starts the purge thread if not already started.
     */
    private static void start() {
        tryStart(PURGE_ENABLED);
    }

    static void tryStart(boolean purgeEnabled) {
        if (purgeEnabled) {
            for (;;) {
                ScheduledExecutorService curr = PURGE_THREAD.get();
                if (curr != null) {
                    return;
                }

                ScheduledExecutorService next = Executors.newScheduledThreadPool(1, PURGE_THREAD_FACTORY);
                if (PURGE_THREAD.compareAndSet(curr, next)) {

                    next.scheduleAtFixedRate(new ScheduledTask(), PURGE_PERIOD_SECONDS, PURGE_PERIOD_SECONDS, TimeUnit.SECONDS);

                    return;
                } else {
                    next.shutdownNow();
                }
            }
        }
    }

    static {
        Properties properties = System.getProperties();

        PurgeProperties pp = new PurgeProperties();
        pp.load(properties);

        PURGE_ENABLED = pp.purgeEnable;
        PURGE_PERIOD_SECONDS = pp.purgePeriod;

        start();
    }

    static final class PurgeProperties {

        boolean purgeEnable;

        int purgePeriod;

        void load(Properties properties) {
            if (properties.containsKey(PURGE_ENABLED_KEY)) {
                purgeEnable = Boolean.parseBoolean(properties.getProperty(PURGE_ENABLED_KEY));
            } else {
                purgeEnable = true;
            }

            if (purgeEnable && properties.containsKey(PURGE_PERIOD_SECONDS_KEY)) {
                try {
                    purgePeriod = Integer.parseInt(properties.getProperty(PURGE_PERIOD_SECONDS_KEY));
                } catch (NumberFormatException ex) {
                    purgePeriod = 1;
                }
            } else {
                purgePeriod = 1;
            }
        }
    }

    /**
     * Creates a ScheduledThreadPoolExecutor with the given factory.
     * @param factory the thread factory
     * @return the ScheduledThreadPoolExecutor
     */
    public static ScheduledThreadPoolExecutor create(ThreadFactory factory) {
        final ScheduledThreadPoolExecutor exec
                = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, factory);
        tryPutIntoPool(PURGE_ENABLED, exec);
        return exec;
    }

    static void tryPutIntoPool(boolean purgeEnabled, ScheduledThreadPoolExecutor exec) {
        if (purgeEnabled) {
            POOLS.put(exec, exec);
        }
    }

    static final class ScheduledTask implements Runnable {
        @Override
        public void run() {
            for (ScheduledThreadPoolExecutor e : new ArrayList<>(POOLS.keySet())) {
                if (e.isShutdown()) {
                    POOLS.remove(e);
                } else {
                    e.purge();
                }
            }
        }
    }
}
