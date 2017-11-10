package org.elasticsearch.hadoop.handler;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Performs the construction and initialization of ErrorHandler instances, implementations of which may be provided
 * by third parties.
 * @param <I> Input type of the {@link ErrorHandler}s being created.
 * @param <O> Type of data accepted by {@link ErrorCollector}s that will interact with this handler.
 */
public abstract class HandlerLoader<I extends Exceptional, O> {

    private enum NamedHandlers {
        FAIL("fail"),
        LOG("log");

        private final String name;

        NamedHandlers(String name) {
            this.name = name;
        }
    }

    private static final Log LOG = LogFactory.getLog(HandlerLoader.class);

    public List<ErrorHandler<I, O>> loadHandlers(Settings settings, Class<? extends ErrorHandler<I, O>> expected) {
        String handlerListPropertyName = getHandlersPropertyName();
        String handlerPropertyPrefix = getHandlerPropertyName();

        List<ErrorHandler<I, O>> handlers = new ArrayList<ErrorHandler<I, O>>();

        List<String> handlerNames = StringUtils.tokenize(settings.getProperty(handlerListPropertyName));
        boolean failureHandlerAdded = false;

        for (String handlerName : handlerNames) {
            Settings handlerSettings = settings.getSettingsView(handlerPropertyPrefix + "." + handlerName);
            ErrorHandler<I, O> handler;
            if (handlerName.equals(NamedHandlers.FAIL.name)) {
                handler = loadBuiltInHandler(NamedHandlers.FAIL, handlerSettings, expected);
                failureHandlerAdded = true;
            } else if (handlerName.equals(NamedHandlers.LOG.name)) {
                handler = loadBuiltInHandler(NamedHandlers.LOG, handlerSettings, expected);
            } else {
                String handlerClassName = settings.getProperty(handlerPropertyPrefix + "." + handlerName);
                handler = ObjectUtils.instantiate(handlerClassName, HandlerLoader.class.getClassLoader());

                if (!expected.isAssignableFrom(handler.getClass())) {
                    throw new EsHadoopIllegalArgumentException("Invalid handler configuration. Expected a handler that " +
                            "extends or is of type [" + expected + "] but was given a handler named [" + handlerName + "] " +
                            "that is an instance of type [" + handler.getClass() + "] which is not compatible.");
                }
            }

            if (handler != null) {
                if (failureHandlerAdded && !handlerName.equals(NamedHandlers.FAIL.name)) {
                    // Handler added after failure handler will most likely never be called.
                    LOG.warn("");
                }

                handler.init(handlerSettings.asProperties());
                handlers.add(handler);
            }
        }

        return handlers;
    }

    /**
     * @return the property name to use for getting the list of handler names
     */
    protected abstract String getHandlersPropertyName();

    /**
     * @return the prefix for creating the properties to use for getting handler specific settings
     */
    protected abstract String getHandlerPropertyName();

    /**
     * Builds the integration specific built-in handler for the job described by the handler name.
     * @param handlerName The built in handler implementation that one should provide
     * @param settings Resolved settings for the handler being returned
     * @param expected Class of the expected handler
     * @return an instance of the built in handler
     */
    protected abstract ErrorHandler<I, O> loadBuiltInHandler(NamedHandlers handlerName, Settings settings, Class<? extends ErrorHandler<I, O>> expected);
}
