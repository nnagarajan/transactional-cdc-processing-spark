package com.technext.demos.txbuffering.transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, transform}

import com.technext.demos.txbuffering.model.spec.{JobModel, StreamTransform}

/** Fills the derived `totalWithTax` of each order element: `totalAmount * 1.2`. */
class TotalWithTaxTransform extends StreamTransform {
  override def apply(records: DataFrame, model: JobModel): DataFrame =
    records.withColumn("orders", transform(col("orders"), o => o.withField("totalWithTax", o.getField("totalAmount") * 1.2)))
}

/** Drops `orderStatus` from the order elements, changing the stream schema. */
class DropStatusTransform extends StreamTransform {
  override def apply(records: DataFrame, model: JobModel): DataFrame =
    records.withColumn("orders", transform(col("orders"), o => o.dropFields("orderStatus")))
}
