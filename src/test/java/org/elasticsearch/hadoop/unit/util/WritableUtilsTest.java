package org.elasticsearch.hadoop.unit.util;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.junit.Test;

import com.google.common.collect.Lists;

public class WritableUtilsTest {
  
  @Test
  public void testListToArrayWritableToList() {
    ArrayList<String> list = Lists.newArrayList("first", "second");
    
    Writable writable = WritableUtils.toWritable(list);
    assertTrue(writable instanceof ArrayWritable);
    
    Object fromWritable = WritableUtils.fromWritable(writable);
    
    assertTrue(fromWritable instanceof List);
    assertEquals(list, fromWritable);
  }
}
