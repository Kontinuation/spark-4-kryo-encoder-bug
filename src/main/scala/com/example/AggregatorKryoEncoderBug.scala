package com.example

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udaf

case class Buf(value: Long)

class MyUDAF extends Aggregator[Long, Buf, Long] {

  override def zero: Buf = Buf(0)

  override def reduce(b: Buf, a: Long): Buf = Buf(b.value + a)

  override def merge(b1: Buf, b2: Buf): Buf = Buf(b1.value + b2.value)

  override def finish(reduction: Buf): Long = reduction.value

  // This fails with "java.io.NotSerializableException: org.apache.spark.sql.catalyst.encoders.KryoSerializationCodec$"
  override def bufferEncoder: Encoder[Buf] = Encoders.kryo[Buf]
  // override def bufferEncoder: Encoder[Buf] = Encoders.javaSerialization[Buf]  // This works
  // override def bufferEncoder: Encoder[Buf] = Encoders.product[Buf]  // This works

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

object AggregatorKryoEncoderBug {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Kryo Encoder Bug")
      .master("local[*]")
      .getOrCreate()

    val myUDAF = udaf(new MyUDAF)
    spark.udf.register("my_udaf", myUDAF)
    val df = spark.range(0, 101).agg(myUDAF(col("id")))
    df.show
  }
}
