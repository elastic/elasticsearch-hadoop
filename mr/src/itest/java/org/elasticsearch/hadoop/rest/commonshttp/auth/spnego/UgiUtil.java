package org.elasticsearch.hadoop.rest.commonshttp.auth.spnego;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.hadoop.EsHadoopException;

/**
 * Utility methods for dealing with Hadoop UGI in integration tests
 */
public class UgiUtil {

    /**
     * Calls the UserGroupInformation.reset() static method by reflection to clear the static
     * configuration values and login users.
     */
    public static void resetUGI() {
        try {
            Method reset = UserGroupInformation.class.getDeclaredMethod("reset");
            reset.setAccessible(true);
            reset.invoke(null);
        } catch (NoSuchMethodException e) {
            throw new EsHadoopException("Could not reset UGI", e);
        } catch (IllegalAccessException e) {
            throw new EsHadoopException("Could not reset UGI", e);
        } catch (InvocationTargetException e) {
            throw new EsHadoopException("Could not reset UGI", e);
        }
    }

}
