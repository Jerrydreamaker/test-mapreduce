package sort.second;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombineKey implements WritableComparable<CombineKey>{
    private int year;
    private int temperature;

    //private static Logger logger=LoggerFactory.getLogger(CombineKey.class);
    public CombineKey() {
    }

    public CombineKey(int year, int temperature) {
        this.year = year;
        this.temperature = temperature;
    }

    public int getYear() {
        return year;
    }


    public int compareTo(CombineKey o) {
        if(year==o.year){
            // 再按温度降序排序
            return o.temperature-temperature;
        }else {
            // 首先按年份升序排序
            return year-o.year;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        //logger.info("Year:"+year);
        dataOutput.writeInt(temperature);
    }

    public void readFields(DataInput dataInput) throws IOException {
        year=dataInput.readInt();
        temperature=dataInput.readInt();
    }
    @Override
    public String toString(){
        return year+" "+temperature;
    }
}
