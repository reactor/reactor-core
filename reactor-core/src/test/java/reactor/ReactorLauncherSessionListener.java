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
