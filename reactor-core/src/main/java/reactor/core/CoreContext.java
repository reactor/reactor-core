package reactor.core;

import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.function.Function;

public interface CoreContext {

    CoreContext contextCapture();

    CoreContext contextWrite(ContextView contextToAppend);

    CoreContext contextWrite(Function<Context, Context> contextModifier);
}
