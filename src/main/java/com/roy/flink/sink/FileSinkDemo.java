package com.roy.flink.sink;

import com.roy.flink.streaming.FileRead;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import java.net.URL;

/**
 * @author roy
 * @date 2021/9/7
 * @desc
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        final URL resource = FileRead.class.getResource("/test.txt");
        final String filePath = resource.getFile();
        final DataStreamSource<String> stream = env.readTextFile(filePath);

        OutputFileConfig outputFileConfig = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();
        final StreamingFileSink<String> streamingfileSink = StreamingFileSink
                .forRowFormat(new Path("D:/ft"), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(outputFileConfig)
                .build();
        stream.addSink(streamingfileSink);

//        final FileSink<String> fileSink = FileSink
//                .forRowFormat(new Path("D:/ft"), new SimpleStringEncoder<String>("UTF-8"))
//                .withOutputFileConfig(outputFileConfig)
//                .build();
//        stream.sinkTo(fileSink);
        env.execute("FileSink");
    }
}
