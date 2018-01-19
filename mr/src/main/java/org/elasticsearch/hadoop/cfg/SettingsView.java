package org.elasticsearch.hadoop.cfg;

import java.io.InputStream;
import java.util.Properties;

/**
 * Provides a limited view over the given settings object.
 */
class SettingsView extends Settings {

    private final Settings parent;
    private final String namespace;

    SettingsView(Settings parent, String namespace) {
        this.parent = parent;
        this.namespace = namespace;
    }

    private String makeKey(String name) {
        return namespace + "." + name;
    }

    @Override
    public InputStream loadResource(String location) {
        throw new UnsupportedOperationException("Settings views do not support loading resources.");
    }

    @Override
    public Settings copy() {
        return new SettingsView(parent.copy(), namespace);
    }

    @Override
    public String getProperty(String name) {
        return parent.getProperty(makeKey(name));
    }

    @Override
    public void setProperty(String name, String value) {
        parent.setProperty(makeKey(name), value);
    }

    @Override
    public Properties asProperties() {
        Properties parentSettings = parent.asProperties();
        Properties viewProperties = new Properties();
        String prefix = namespace + ".";
        for (Object keyObject : parentSettings.keySet()) {
            String key = keyObject.toString();
            if (key.startsWith(prefix) && !key.equals(prefix)) {
                String newKey = key.substring(prefix.length());
                viewProperties.put(newKey, parentSettings.getProperty(key));
            }
        }
        return viewProperties;
    }
}
