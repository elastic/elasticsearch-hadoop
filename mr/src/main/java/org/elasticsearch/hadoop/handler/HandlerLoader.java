package org.elasticsearch.hadoop.handler;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Performs the construction and initialization of ErrorHandler instances, implementations of which may be provided
 * by third parties.
 * @param <E> The {@link ErrorHandler}s being created.
 */
public abstract class HandlerLoader<E extends ErrorHandler> implements SettingsAware {

    public enum NamedHandlers {
        FAIL("fail"),
        LOG("log");

        private final String name;

        NamedHandlers(String name) {
            this.name = name;
        }
    }

    private static final Log LOG = LogFactory.getLog(HandlerLoader.class);

    private final Class<E> expected;
    private Settings settings;

    public HandlerLoader(Class<E> expected) {
        this.expected = expected;
    }

    @Override
    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public List<E> loadHandlers() {
        String handlerListPropertyName = getHandlersPropertyName();
        String handlerPropertyPrefix = getHandlerPropertyName();

        List<E> handlers = new ArrayList<E>();

        List<String> handlerNames = StringUtils.tokenize(settings.getProperty(handlerListPropertyName));
        boolean failureHandlerAdded = false;

        for (String handlerName : handlerNames) {
            Settings handlerSettings = settings.getSettingsView(handlerPropertyPrefix + "." + handlerName);
            E handler;
            if (handlerName.equals(NamedHandlers.FAIL.name)) {
                handler = loadBuiltInHandler(NamedHandlers.FAIL);
                failureHandlerAdded = true;
            } else if (handlerName.equals(NamedHandlers.LOG.name)) {
                handler = loadBuiltInHandler(NamedHandlers.LOG);
            } else {
                String handlerClassName = settings.getProperty(handlerPropertyPrefix + "." + handlerName);
                handler = ObjectUtils.instantiate(handlerClassName, HandlerLoader.class.getClassLoader());
            }

            if (handler != null) {
                if (!expected.isAssignableFrom(handler.getClass())) {
                    throw new EsHadoopIllegalArgumentException("Invalid handler configuration. Expected a handler that " +
                            "extends or is of type [" + expected + "] but was given a handler named [" + handlerName + "] " +
                            "that is an instance of type [" + handler.getClass() + "] which is not compatible.");
                }

                if (failureHandlerAdded && !handlerName.equals(NamedHandlers.FAIL.name)) {
                    // Handler added after failure handler will most likely never be called.
                    LOG.warn(String.format("Found error handler named [%s] ordered after the built in failure handler. This handler " +
                            "will never be called as the failure handler preceding it will consume any and all errors. " +
                            "Consider reordering your handlers in the [%s] property.", handlerName, handlerListPropertyName));
                }

                handler.init(handlerSettings.asProperties());
                handlers.add(handler);
            }
        }

        if (!failureHandlerAdded) {
            // Add the failure handler at the very end of the list as a fail safe.
            E handler = loadBuiltInHandler(NamedHandlers.FAIL);
            Settings handlerSettings = settings.getSettingsView(handlerPropertyPrefix + "." + NamedHandlers.FAIL.name);
            handler.init(handlerSettings.asProperties());
            handlers.add(handler);
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
     * @return an instance of the built in handler
     */
    protected abstract E loadBuiltInHandler(NamedHandlers handlerName);
}
