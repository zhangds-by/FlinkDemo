package com.roy.flink.table;

import com.roy.flink.beans.Stock;
import com.roy.flink.window.WindowAssignerDemo;
import javafx.scene.control.Tab;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author roy
 * @date 2021/9/13
 * @desc 在DataStream转为Table时定义事件时间。
 */
public class TableWatermarkDemo2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database").build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        //如果从文件读取，数据一次就处理完了。
        String filePath = WindowAssignerDemo.class.getResource("/stock.txt").getFile();
        final DataStreamSource<String> dataStream = env.readTextFile(filePath, "UTF-8");
        final SingleOutputStreamOperator<Stock> stockStream = dataStream.map(new MapFunction<String, Stock>() {
            @Override
            public Stock map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
            }
        });
        //KEY1：定义一个WatermarkStrategy。Watermark延迟2秒
        WatermarkStrategy<Stock> watermarkStrategy= WatermarkStrategy.<Stock>forBoundedOutOfOrderness(Duration.ofMillis(2))
                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()));
        final SingleOutputStreamOperator<Stock> etStream = stockStream.assignTimestampsAndWatermarks(watermarkStrategy);
        //将事件时间定义成一个新的字段 eventtime
        final Table table = tableEnv.fromDataStream(etStream, $("id"), $("price"),$("stockName"), $("eventtime").rowtime());
//        final Table selectedTable = table.groupBy($("stockName"))
//                .select($("stockName"), $("price").max().as("maxPrice"));
//
//        tableEnv.toRetractStream(selectedTable, TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){}))
//                .print("selectedTable");
        //查找eventtime字段。
        final Table selectedTable = table
                .select($("id"), $("price"),$("eventtime"));
//
//        tableEnv.toRetractStream(selectedTable, TypeInformation.of(new TypeHint<Tuple3<String, Double, Timestamp>>(){}))
//                .print("selectedTable");
        tableEnv.toAppendStream(selectedTable,TypeInformation.of(new TypeHint<Tuple3<String, Double, Timestamp>>(){}))
                .print("selectedTable");


        env.execute("TableWatermarkDemo2");
    }
}
