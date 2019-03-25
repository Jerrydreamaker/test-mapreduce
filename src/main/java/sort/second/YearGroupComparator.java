package sort.second;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearGroupComparator extends WritableComparator {
    YearGroupComparator(){
        super(CombineKey.class,true);
    }

    @Override
    public int compare(WritableComparable c1, WritableComparable c2){
        return ((CombineKey)c1).getYear()-((CombineKey)c2).getYear();

    }

}
