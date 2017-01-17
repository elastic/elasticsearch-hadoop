package org.elasticsearch.hadoop.util.encoding;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class HttpEncodingToolsTest {
    @Test
    public void encodeUri() throws Exception {
        assertThat(HttpEncodingTools.encodeUri("http://127.0.0.1:9200/tést/typé/_search?x=x;x&y=y:¥&z=z|Ω"),
                is("http://127.0.0.1:9200/t%C3%A9st/typ%C3%A9/_search?x=x;x&y=y:%C2%A5&z=z%7C%CE%A9"));
    }

    @Test
    public void encodePath() throws Exception {
        assertThat(HttpEncodingTools.encodePath(""),
                is(""));

        assertThat(HttpEncodingTools.encodePath("/a"),
                is("/a"));

        assertThat(HttpEncodingTools.encodePath("/a/b"),
                is("/a/b"));

        assertThat(HttpEncodingTools.encodePath("/a/b/_c"),
                is("/a/b/_c"));

        assertThat(HttpEncodingTools.encodePath("/å∫ç∂/˚¬µñøπœ/_search"),
                is("/%C3%A5%E2%88%AB%C3%A7%E2%88%82/%CB%9A%C2%AC%C2%B5%C3%B1%C3%B8%CF%80%C5%93/_search"));
    }

    @Test
    public void encode() throws Exception {
        assertThat(HttpEncodingTools.encode("?x=x&y=y|¥&z=Ω"),
                is("%3Fx%3Dx%26y%3Dy%7C%C2%A5%26z%3D%CE%A9"));
    }

    @Test
    public void decode() throws Exception {
        assertThat(HttpEncodingTools.decode("http://127.0.0.1:9200/%C3%A5%E2%88%AB%C3%A7%E2%88%82/%CB%9A%C2%AC%C2%B5%C3%B1%C3%B8%CF%80%C5%93/_search?x=x&y=y%7C%C2%A5&z=%CE%A9"),
                is("http://127.0.0.1:9200/å∫ç∂/˚¬µñøπœ/_search?x=x&y=y|¥&z=Ω"));
    }

    @Test
    public void testMultiAmpersandEscapeSimple() {
        assertThat(HttpEncodingTools.concatenateAndUriEncode(Arrays.asList("&a", "$b", "#c", "!d", "/e", ":f"), ","),
                is("%26a,%24b,%23c,%21d,%2Fe,%3Af"));
    }

    @Test
    public void tokenizeAndUriDecode() throws Exception {
        assertThat(HttpEncodingTools.tokenizeAndUriDecode("%26a,%24b,%23c,%21d,%2Fe,%3Af",","),
                containsInAnyOrder("&a", "$b", "#c", "!d", "/e", ":f"));
    }

    @Test
    public void testSingleAmpersandEscape() {
        String uri = HttpEncodingTools.encode("&c");
        assertThat(uri, is("%26c"));
    }

    @Test
    public void testEscapePercent() {
        String uri = HttpEncodingTools.encode("%s");
        assertThat(uri, is("%25s"));
    }

}