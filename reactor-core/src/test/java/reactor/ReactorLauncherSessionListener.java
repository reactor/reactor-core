/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;
import reactor.test.AssertionsUtils;
import reactor.test.util.LoggerUtils;
import reactor.util.Loggers;

public class ReactorLauncherSessionListener implements LauncherSessionListener {

    /**
     * Reset the {@link Loggers} factory to defaults suitable for reactor-core tests.
     * Notably, it installs an indirection via {@link LoggerUtils#useCurrentLoggersWithCapture()}.
     */
    public static void resetLoggersFactory() {
        Loggers.resetLoggerFactory();
        LoggerUtils.useCurrentLoggersWithCapture();
    }

    @Override
    public void launcherSessionOpened(LauncherSession session) {
        AssertionsUtils.installAssertJTestRepresentation();
        ReactorLauncherSessionListener.resetLoggersFactory();
    }
}
