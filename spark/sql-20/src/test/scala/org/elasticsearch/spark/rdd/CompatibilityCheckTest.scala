package org.elasticsearch.spark.rdd

import org.junit.Test

/**
  * Created by james.baiera on 8/17/16.
  */
class CompatibilityCheckTest {

  @Test
  def checkCompatibility: Unit = {
    CompatUtils.checkSparkLibraryCompatibility(true)
  }

}
