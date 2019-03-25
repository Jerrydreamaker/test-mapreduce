package sort.second;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义数据分区，默认采用 Hash 分区。
 */
public class YearPartitioner extends Partitioner<CombineKey, NullWritable> {
    @Override
    public int getPartition(CombineKey key, NullWritable value, int numPartitions) {
        return key.getYear()%numPartitions;
    }
}
