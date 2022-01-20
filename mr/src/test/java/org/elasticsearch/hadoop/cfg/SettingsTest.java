package org.elasticsearch.hadoop.cfg;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class SettingsTest {
    @Test
    public void getXOpaqueId() throws Exception {
        TestSettings testSettings = new TestSettings();
        String opaqueId1 = "This is an opaque ID";
        testSettings.setOpaqueId(opaqueId1);
        assertEquals(opaqueId1, testSettings.getOpaqueId());
        testSettings.setOpaqueId("This one\n has newlines\r\n and a carriage return");
        assertEquals("This one has newlines and a carriage return", testSettings.getOpaqueId());
        testSettings.setOpaqueId("This oñe has a non-ascii character");
        assertEquals("This oe has a non-ascii character", testSettings.getOpaqueId());
    }

    public static class TestSettings extends Settings {
        private Map<String, String> actualSettings = new HashMap();
        @Override
        public InputStream loadResource(String location) {
            return null;
        }

        @Override
        public Settings copy() {
            return null;
        }

        @Override
        public String getProperty(String name) {
            return actualSettings.get(name);
        }

        @Override
        public void setProperty(String name, String value) {
            actualSettings.put(name, value);
        }

        @Override
        public Properties asProperties() {
            return null;
        }
    }
}
