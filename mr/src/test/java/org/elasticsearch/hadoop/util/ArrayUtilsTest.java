package org.elasticsearch.hadoop.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class ArrayUtilsTest {
    @Test
    public void sliceEqualsIdentity() throws Exception {
        byte[] empty = new byte[0];
        assertTrue(ArrayUtils.sliceEquals(
                empty, 0, 0,
                empty, 0, 0
        ));
    }

    @Test
    public void sliceEqualsNullNull() throws Exception {
        assertTrue(ArrayUtils.sliceEquals(
                null, 0, 0,
                null, 0, 0
        ));
    }

    @Test
    public void sliceEqualsEmptyNull() throws Exception {
        assertFalse(ArrayUtils.sliceEquals(
                new byte[0], 0, 0,
                null, 0, 0
        ));
    }

    @Test
    public void sliceEqualsNullEmpty() throws Exception {
        assertFalse(ArrayUtils.sliceEquals(
                null, 0, 0,
                new byte[0], 0, 0
        ));
    }

    @Test
    public void sliceEqualsSingleSingle() throws Exception {
        assertTrue(ArrayUtils.sliceEquals(
                new byte[]{0}, 0, 1,
                new byte[]{0}, 0, 1
        ));
    }

    @Test
    public void sliceEqualsMultiMulti() throws Exception {
        assertTrue(ArrayUtils.sliceEquals(
                new byte[]{0,1,2,3,4,5}, 0, 6,
                new byte[]{0,1,2,3,4,5}, 0, 6
        ));
    }


    @Test
    public void sliceEqualsSlicedMulti() throws Exception {
        assertTrue(ArrayUtils.sliceEquals(
                new byte[]{10,10,2,3,0,0}, 2, 2,
                new byte[]{20,20,2,3,4,4}, 2, 2
        ));
    }

    @Test
    public void sliceEqualsSlicedMultiSkewed() throws Exception {
        assertTrue(ArrayUtils.sliceEquals(
                new byte[]{0,1,2,3,0,0}, 0, 4,
                new byte[]{0,0,0,1,2,3}, 2, 4
        ));
    }

    @Test
    public void sliceEqualsSlicedMultiOffset() throws Exception {
        assertFalse(ArrayUtils.sliceEquals(
                new byte[]{10,10,2,3,0,0}, 1, 2,
                new byte[]{20,20,2,3,4,4}, 1, 2
        ));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void sliceEqualsLengthOOB() throws Exception {
        ArrayUtils.sliceEquals(
                new byte[]{0}, 0, 2,
                new byte[]{0}, 0, 1
        );
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void sliceEqualsLengthOOB2() throws Exception {
        ArrayUtils.sliceEquals(
                new byte[]{0}, 0, 1,
                new byte[]{0}, 0, 2
        );
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void sliceEqualsOffsetOOB() throws Exception {
        ArrayUtils.sliceEquals(
                new byte[]{0}, 1, 1,
                new byte[]{0}, 0, 1
        );
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void sliceEqualsOffsetOOB2() throws Exception {
        ArrayUtils.sliceEquals(
                new byte[]{0}, 0, 1,
                new byte[]{0}, 1, 1
        );
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void sliceEqualsOffsetLengthOOB() throws Exception {
        ArrayUtils.sliceEquals(
                new byte[]{0,1}, 1, 2,
                new byte[]{0,1}, 1, 1
        );
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void sliceEqualsOffsetLengthOOB2() throws Exception {
        ArrayUtils.sliceEquals(
                new byte[]{0,1}, 1, 1,
                new byte[]{0,1}, 1, 2
        );
    }
}