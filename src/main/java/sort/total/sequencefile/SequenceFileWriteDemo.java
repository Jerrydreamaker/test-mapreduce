package sort.total.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * 向 HDFS 制造 SequenceFile 文件。
 */
public class SequenceFileWriteDemo {
    public static void main(String[] args) throws IOException {
        Configuration configuration=new Configuration();
        Path path=new Path("hdfs://localhost:9000/test/test.seq");
        FileSystem fileSystem=path.getFileSystem(configuration);
        SequenceFile.Writer writer= SequenceFile.createWriter(fileSystem,configuration,
                path, IntWritable.class, Text.class);
        IntWritable key=new IntWritable();
        Text value=new Text();
        for (int i=1;i<=100;i++){
            key.set(i);
            value.set("value"+i);
            writer.append(key,value);
        }
        writer.close();
        System.out.println("Finish!");

    }
}
