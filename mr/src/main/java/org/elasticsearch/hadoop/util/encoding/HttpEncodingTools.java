package org.elasticsearch.hadoop.util.encoding;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.util.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Helpful methods for encoding strings into "application/x-www-form-urlencoded" format
 * for safe usage in URLs/URIs.
 */
public final class HttpEncodingTools {

    private HttpEncodingTools() {
        /* No Constructor */
    }

    /**
     * Splits the given string on the first '?' then encodes the first half as a path (ignoring slashes and colons)
     * and the second half as a query segment (ignoring questionmarks, equals signs, etc...).
     *
     * @deprecated Prefer to use {@link HttpEncodingTools#encode(String)} instead for encoding specific
     * pieces of the URI. This method does not escape certain reserved characters, like '/', ':', '=', and '?'.
     * As such, this is not safe to use on URIs that may contain these reserved characters in the wrong places.
     */
    @Deprecated
    public static String encodeUri(String uri) {
        try {
            return URIUtil.encodePathQuery(uri);
        } catch (URIException ex) {
            throw new EsHadoopIllegalArgumentException("Cannot escape uri [" + uri + "]", ex);
        }
    }

    /**
     * Encodes characters in the string except for those allowed in an absolute path.
     *
     * @deprecated Prefer to use {@link HttpEncodingTools#encode(String)} instead for encoding specific
     * pieces of the URI. This method does not escape certain reserved characters, like '/' and ':'.
     * As such, this is not safe to use on paths that may contain these reserved characters in the wrong places.
     */
    @Deprecated
    public static String encodePath(String path) {
        try {
            return URIUtil.encodePath(path, "UTF-8");
        } catch (URIException ex) {
            throw new EsHadoopIllegalArgumentException("Cannot encode path segment [" + path + "]", ex);
        }
    }

    /**
     * Encodes all characters in the string into "application/x-www-form-urlencoded" format.
     *
     * Allowed characters in this case are [a-zA-z0-9] and "_", "-", ".", "*", and space (which becomes a '+' sign)
     *
     * @param value UTF-8 string to encode.
     * @return String in "application/x-www-form-urlencoded" format.
     */
    public static String encode(String value) {
        try {
            // TODO: Potentially fix the plus signs that come out of encoding to be "%20"
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new EsHadoopIllegalArgumentException("Cannot encode string [" + value + "]", e);
        }
    }

    /**
     * Decodes a "application/x-www-form-urlencoded" format string back to a regular string value,
     * returning the "%XX" codes back into UTF-8 Characters.
     * @param encoded String in "application/x-www-form-urlencoded" format.
     * @return A UTF-8 formatted String.
     */
    public static String decode(String encoded) {
        try {
            return URLDecoder.decode(encoded, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new EsHadoopIllegalArgumentException("Cannot decode string [" + encoded + "]", e);
        }
    }

    /**
     * Encodes each string value of the list and concatenates the results using the supplied delimiter.
     *
     * @param list To be encoded and concatenated.
     * @param delimiter Separator for concatenation.
     * @return Concatenated and encoded string representation.
     */
    public static String concatenateAndUriEncode(Collection<?> list, String delimiter) {
        Collection<String> escaped = new ArrayList<String>();

        if (list != null) {
            for (Object object : list) {
                escaped.add(encode(object.toString()));
            }
        }
        return StringUtils.concatenate(escaped, delimiter);
    }

    /**
     * Given a string of "application/x-www-form-urlencoded" format values, tokenize it and decode the values.
     *
     * @param string A "application/x-www-form-urlencoded" format string.
     * @param delimiters delimiters to use for tokenizing the given string.
     * @return List of decoded values (UTF-8) from the original string.
     */
    public static List<String> tokenizeAndUriDecode(String string, String delimiters) {
        List<String> tokenize = StringUtils.tokenize(string, delimiters, true, true);
        List<String> decoded = new ArrayList<String>(tokenize.size());

        for (String token : tokenize) {
            decoded.add(decode(token));
        }

        return decoded;
    }
}
