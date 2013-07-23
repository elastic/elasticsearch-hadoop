/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.hadoop.serialization.SerializationException;

/**
 * Utility class used internally for the Pig support.
 */
public abstract class IOUtils {

    public static String serializeToBase64(Serializable object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.close();
        return new String(Base64.encodeBase64(baos.toByteArray(), false, true));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T deserializeFromBase64(String data) {
        byte[] rawData = Base64.decodeBase64(data.getBytes());
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(rawData));
            Object o = ois.readObject();
            ois.close();
            return (T) o;
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("cannot deserialize object", ex);
        } catch (IOException ex) {
            throw new SerializationException("cannot deserialize object", ex);
        }
    }
}
