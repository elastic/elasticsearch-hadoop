package org.elasticsearch.spark.sql

import org.apache.spark.sql.sources.DataSourceRegister

// extension of DefaultSource (which is Spark 1.3 and 1.4 compatible)
// for Spark 1.5 compatibility
// since the class is loaded through META-INF/services we can decouple the two
// to have Spark 1.5 byte-code loaded lazily
class DefaultSource15 extends DefaultSource with DataSourceRegister {

    override def shortName(): String = "es"
}