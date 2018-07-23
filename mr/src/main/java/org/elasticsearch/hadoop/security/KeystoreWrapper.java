/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.hadoop.security;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;

public class KeystoreWrapper {

    private static final String PKCS12 = "PKCS12";
    private static final String AES = "AES";

    // TODO: Eventually support password protected keystores when Elasticsearch does.
    private static final String DEFAULT_PASS = "";

    private final KeyStore keyStore;
    private final KeyStore.PasswordProtection protection;

    private KeystoreWrapper(InputStream inputStream, String type, String password) throws EsHadoopSecurityException, IOException {
        Assert.notNull(password, "Password should not be null");
        try {
            char[] pwd = password.toCharArray();
            protection = new KeyStore.PasswordProtection(pwd);
            keyStore = KeyStore.getInstance(type);
            keyStore.load(inputStream, pwd);
        } catch (CertificateException e) {
            throw new EsHadoopSecurityException("Could not create keystore", e);
        } catch (NoSuchAlgorithmException e) {
            throw new EsHadoopSecurityException("Could not create keystore", e);
        } catch (KeyStoreException e) {
            throw new EsHadoopSecurityException("Could not create keystore", e);
        }
    }

    public void setSecureSetting(String alias, String key) throws EsHadoopSecurityException {
        SecretKey spec = new SecretKeySpec(key.getBytes(), AES);
        KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(spec);
        try {
            keyStore.setEntry(alias, entry, protection);
        } catch (KeyStoreException e) {
            throw new EsHadoopSecurityException(String.format("Could not store secret key (alias : [%s]) in keystore", alias), e);
        }
    }

    public void removeSecureSetting(String alias) throws EsHadoopSecurityException {
        try {
            keyStore.deleteEntry(alias);
        } catch (KeyStoreException e) {
            throw new EsHadoopSecurityException(String.format("Could not delete secret key (alias : [%s]) from keystore", alias), e);
        }
    }

    public String getSecureSetting(String alias) throws EsHadoopSecurityException {
        try {
            if (!keyStore.containsAlias(alias)) {
                return null;
            }
            KeyStore.Entry entry = keyStore.getEntry(alias, protection);
            KeyStore.SecretKeyEntry secretKeyEntry = ((KeyStore.SecretKeyEntry) entry);
            return new String(secretKeyEntry.getSecretKey().getEncoded());
        } catch (NoSuchAlgorithmException e) {
            throw new EsHadoopSecurityException(String.format("Could not read alias [%s] from keystore", alias), e);
        } catch (UnrecoverableEntryException e) {
            throw new EsHadoopSecurityException(String.format("Could not read alias [%s] from keystore", alias), e);
        } catch (KeyStoreException e) {
            throw new EsHadoopSecurityException(String.format("Could not read alias [%s] from keystore", alias), e);
        }
    }

    public boolean containsEntry(String alias) throws EsHadoopSecurityException {
        try {
            return keyStore.containsAlias(alias);
        } catch (KeyStoreException e) {
            throw new EsHadoopSecurityException(String.format("Could not read existence of alias [%s]", alias), e);
        }
    }

    public List<String> listEntries() throws EsHadoopSecurityException {
        try {
            List<String> entries = new ArrayList<String>(keyStore.size());
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                entries.add(alias);
            }
            return entries;
        } catch (KeyStoreException e) {
            throw new EsHadoopSecurityException("Could not read aliases from keystore", e);
        }
    }

    public void saveKeystore(OutputStream outputStream) throws EsHadoopSecurityException, IOException {
        try {
            keyStore.store(outputStream, protection.getPassword());
        } catch (KeyStoreException e) {
            throw new EsHadoopSecurityException("Could not persist keystore", e);
        } catch (NoSuchAlgorithmException e) {
            throw new EsHadoopSecurityException("Could not persist keystore", e);
        } catch (CertificateException e) {
            throw new EsHadoopSecurityException("Could not persist keystore", e);
        }
    }

    public void saveKeystore(String path) throws EsHadoopSecurityException, IOException {
        OutputStream stream = null;
        try {
            stream = new FileOutputStream(new File(path));
            saveKeystore(stream);
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    public static KeystoreBuilder loadStore(String path) {
        return new KeystoreBuilder(path);
    }

    public static KeystoreBuilder loadStore(InputStream stream) {
        return new KeystoreBuilder(stream);
    }

    public static KeystoreBuilder newStore() {
        return new KeystoreBuilder();
    }

    public static final class KeystoreBuilder {
        private String type;
        private String password;
        private String path;
        private InputStream keystoreFile;

        private KeystoreBuilder(InputStream keystoreFile) {
            this.keystoreFile = keystoreFile;
        }

        private KeystoreBuilder(String path) {
            this.path = path;
        }

        private KeystoreBuilder() {
            // New keystore
        }

        public KeystoreBuilder setType(String type) {
            this.type = type;
            return this;
        }

        public KeystoreBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public KeystoreWrapper build() throws EsHadoopSecurityException, IOException {
            if (StringUtils.hasText(path)) {
                try {
                    keystoreFile = IOUtils.open(path);
                    if (keystoreFile == null) {
                        throw new EsHadoopIllegalArgumentException(String.format("Could not locate [%s] on classpath", path));
                    }
                } catch (Exception e) {
                    throw new EsHadoopIllegalArgumentException(String.format("Expected to find keystore file at [%s] but " +
                            "was unable to. Make sure that it is available on the classpath, or if not, that you have " +
                            "specified a valid file URI.", path));
                }
            }
            if (!StringUtils.hasText(type)) {
                type = PKCS12;
            }
            if (!StringUtils.hasText(password)) {
                password = DEFAULT_PASS;
            }
            return new KeystoreWrapper(keystoreFile, type, password);
        }
    }
}
