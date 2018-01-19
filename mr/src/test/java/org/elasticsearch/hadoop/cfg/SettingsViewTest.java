package org.elasticsearch.hadoop.cfg;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SettingsViewTest {
    @Test
    public void getProperty() throws Exception {
        Settings parentSettings = new PropertiesSettings();
        parentSettings.setProperty("test.name1.key1", "val1.1");
        parentSettings.setProperty("test.name1.key2", "val1.2");
        parentSettings.setProperty("test.name1.key3", "val1.3");
        parentSettings.setProperty("test.name2.key1", "val2.1");
        parentSettings.setProperty("test.name2.key2", "val2.2");

        Settings view = parentSettings.getSettingsView("test.name1");

        Assert.assertEquals("val1.1", view.getProperty("key1"));
        Assert.assertEquals("val1.2", view.getProperty("key2"));
        Assert.assertEquals("val1.3", view.getProperty("key3"));
    }

    @Test
    public void setProperty() throws Exception {
        Settings parentSettings = new PropertiesSettings();

        Settings view = parentSettings.getSettingsView("test.name1");
        view.setProperty("key1", "val1.1");

        Assert.assertEquals("val1.1", parentSettings.getProperty("test.name1.key1"));
    }

    @Test
    public void asProperties() throws Exception {
        Settings parentSettings = new PropertiesSettings();
        parentSettings.setProperty("test.name1.key1", "val1.1");
        parentSettings.setProperty("test.name1.key2", "val1.2");
        parentSettings.setProperty("test.name1.key3", "val1.3");
        parentSettings.setProperty("test.name2.key1", "val2.1");
        parentSettings.setProperty("test.name2.key2", "val2.2");

        Settings view = parentSettings.getSettingsView("test.name1");
        Properties viewProps = view.asProperties();

        Assert.assertEquals("val1.1", viewProps.getProperty("key1"));
        Assert.assertEquals("val1.2", viewProps.getProperty("key2"));
        Assert.assertEquals("val1.3", viewProps.getProperty("key3"));
    }

    @Test
    public void copy() throws Exception {
        Settings parentSettings = new PropertiesSettings();
        parentSettings.setProperty("test.name1.key1", "val1.1");
        parentSettings.setProperty("test.name1.key2", "val1.2");
        parentSettings.setProperty("test.name1.key3", "val1.3");
        parentSettings.setProperty("test.name2.key1", "val2.1");
        parentSettings.setProperty("test.name2.key2", "val2.2");

        Settings view = parentSettings.getSettingsView("test.name1");
        Settings viewCopy = view.copy();

        Assert.assertEquals("val1.1", viewCopy.getProperty("key1"));
        Assert.assertEquals("val1.2", viewCopy.getProperty("key2"));
        Assert.assertEquals("val1.3", viewCopy.getProperty("key3"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void loadResource() throws Exception {
        new PropertiesSettings().getSettingsView("test").loadResource("/does/not/exist");
        Assert.fail("Should have failed.");
    }
}
