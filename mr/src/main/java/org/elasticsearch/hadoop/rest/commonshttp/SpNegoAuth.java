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

package org.elasticsearch.hadoop.rest.commonshttp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.ietf.jgss.Oid;

public class SpNegoAuth {

    public void doThings() throws Exception {
        DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(new byte[0]));
        DataOutputStream outStream = new DataOutputStream(new ByteArrayOutputStream());

        GSSManager gssManager = GSSManager.getInstance();
        Oid spnegoOid = new Oid("1.3.6.1.5.5.2");
        GSSName serverName = gssManager.createName("serverPrinc", GSSName.NT_HOSTBASED_SERVICE, spnegoOid);
        GSSContext context = gssManager.createContext(serverName, spnegoOid, null, GSSContext.DEFAULT_LIFETIME);
        context.requestMutualAuth(true);
        context.requestConf(true);

        byte[] token = new byte[0];
        while (!context.isEstablished()) {
            token = context.initSecContext(token, 0, token.length);
            outStream.writeInt(token.length);
            outStream.write(token);
            outStream.flush();

            // Check if we're done
            if (!context.isEstablished()) {
                token = new byte[inStream.readInt()];
                inStream.readFully(token);
            }
        }

        // Established!

        // This requests confidentiality?
        MessageProp messageProp = new MessageProp(0, true);

        byte[] sendMessage = new byte[0];

        // Create encrypted message
        token = context.wrap(sendMessage, 0, sendMessage.length, messageProp);

        // Write to server
        outStream.writeInt(token.length);
        outStream.write(token);
        outStream.flush();

        // Read from server
        token = new byte[inStream.readInt()];
        inStream.readFully(token);

        // Unwrap encrypted message
        byte[] response = context.unwrap(token, 0, token.length, messageProp);

        // Dispose context
        context.dispose();
    }
}
