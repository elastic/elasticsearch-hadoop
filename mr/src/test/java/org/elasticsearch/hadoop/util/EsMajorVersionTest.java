package org.elasticsearch.hadoop.util;

import org.elasticsearch.Version;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EsMajorVersionTest {
    private static final List<Version> SORTED_VERSIONS;

    static {
        Field[] declaredFields = Version.class.getFields();
        Set<Integer> ids = new HashSet<Integer>();
        for (Field field : declaredFields) {
            final int mod = field.getModifiers();
            if (Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                if (field.getType() == Version.class) {
                    try {
                        Version object = (Version) field.get(null);
                        ids.add(object.id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        List<Integer> idList = new ArrayList<Integer>(ids);
        Collections.sort(idList);
        List<Version> version = new ArrayList<Version>();
        for (Integer integer : idList) {
            version.add(Version.fromId(integer));
        }
        SORTED_VERSIONS = Collections.unmodifiableList(version);
    }


    @Test
    public void testVersionFromString() {
        for (int i = 0; i < SORTED_VERSIONS.size(); i++) {
            Version official = SORTED_VERSIONS.get(i);
            EsMajorVersion version = EsMajorVersion.parse(official.toString());
            assertThat(version.major,
                    equalTo(official.major));
            assertTrue(version.onOrAfter(version));
            assertTrue(version.equals(version));
            for (int j = i + 1; j < SORTED_VERSIONS.size(); j++) {
                Version cmp_official = SORTED_VERSIONS.get(j);
                EsMajorVersion cmp_version = EsMajorVersion.parse(cmp_official.toString());
                assertThat(cmp_version.after(version), equalTo(cmp_official.major != official.major));
                assertTrue(cmp_version.onOrAfter(version));
                assertFalse(cmp_version.equals(version));
            }

            for (int j = i - 1; j >= 0; j--) {
                Version cmp_official = SORTED_VERSIONS.get(j);
                EsMajorVersion cmp_version = EsMajorVersion.parse(cmp_official.toString());
                assertThat(cmp_version.before(version), equalTo(cmp_official.major != official.major));
                assertTrue(cmp_version.onOrBefore(version));
                assertFalse(cmp_version.equals(version));
            }
        }
    }

    @Test
    public void testLatestIsCurrent() {
        assertThat(EsMajorVersion.LATEST.major, equalTo(Version.CURRENT.major));
    }
}