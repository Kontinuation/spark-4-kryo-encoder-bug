package com.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.iterators.SingletonIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class FlatMapGroupStateKryoEncoderBug {

  public static void main(String[] args) throws TimeoutException, StreamingQueryException {
    SparkSession.Builder b = SparkSession.builder();

    SparkConf conf = new SparkConf()
        .set("spark.sql.shuffle.partitions", "2")
        .setMaster("local[2]");

    SparkSession session = b.config(conf)
        .getOrCreate();

    DataStreamReader reader = session.readStream();

    Dataset<Row> ds = reader.format("rate")
        .option("rowsPerSecond", 1).load();

    Dataset<Input> mapped = ds.map((MapFunction<Row, Input>) e -> new Input(new Key((int) (e.getLong(1) % 10)), e.getLong(1), e.getTimestamp(0)), Encoders.kryo(Input.class));

    KeyValueGroupedDataset<Key, Input> keyed = mapped.groupByKey((MapFunction<Input, Key>) Input::getKey, Encoders.kryo(Key.class));

    Dataset<Result> res = flatMapGroupState(keyed);

    res.writeStream()
        .format("console")
        .start()
        .awaitTermination();
  }

  private static Dataset<Result> flatMapGroupState(KeyValueGroupedDataset<Key, Input> keyed) {
    return keyed.flatMapGroupsWithState(new FlatMapGroupsWithStateFunction<Key, Input, State, Result>() {

      @Override
      public Iterator<Result> call(Key key, Iterator<Input> iterator, GroupState<State> groupState) throws Exception {
        State state = groupState.getOption().getOrElse(() -> new State(key, 0));

        iterator.forEachRemaining(pojo -> state.setCount(state.getCount() + 1));

        groupState.update(state);

        return new SingletonIterator<>(new Result(state.getKey(), state.getCount()));
      }
    }, OutputMode.Append(), Encoders.kryo(State.class), Encoders.kryo(Result.class), GroupStateTimeout.NoTimeout());
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Key implements Serializable {
    int value;
  }

  @AllArgsConstructor
  @Data
  @NoArgsConstructor
  public static class Result implements Serializable {
    private Key key;
    private long count;
  }

  @AllArgsConstructor
  @Data
  @NoArgsConstructor
  public static class State implements Serializable {
    private Key key;
    private long count;
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Input implements Serializable {
    Key key;
    long index;
    Timestamp timestamp;
  }
}