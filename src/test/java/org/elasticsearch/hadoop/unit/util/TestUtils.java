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
package org.elasticsearch.hadoop.unit.util;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;

public class TestUtils {

    /**
     * Hack to allow Hadoop client to run on windows (which otherwise fails due to some permission problem).
     */
    public static void hackHadoopStagingOnWin() {
        // do the assignment only on Windows systems
        if (isWindows()) {
            // 0655 = -rwxr-xr-x
            JobSubmissionFiles.JOB_DIR_PERMISSION.fromShort((short) 0650);
            JobSubmissionFiles.JOB_FILE_PERMISSION.fromShort((short) 0650);

            // handle jar permissions as well - temporarily disable for CDH 4 / YARN
            try {
                Class<?> tdcm = Class.forName("org.apache.hadoop.filecache.TrackerDistributedCacheManager");
                FsPermission perm = (FsPermission) FieldUtils.readStaticField(tdcm, "PUBLIC_CACHE_OBJECT_PERM", true);
                perm.fromShort((short) 0650);
            } catch (ClassNotFoundException cnfe) {
                //ignore
                return;
            } catch (Exception ex) {
                LogFactory.getLog(TestUtils.class).warn("Cannot set permission for TrackerDistributedCacheManager", ex);
            }
        }
    }

    public static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("win");
    }
}
