package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义数据分区，默认采用 Hash 分区。
 */
public class MyPartitioner extends Partitioner<Text, IntWritable> {
    public int getPartition(Text text, IntWritable intWritable, int i) {
        return 1;
    }
}
