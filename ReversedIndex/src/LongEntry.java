import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LongEntry implements Writable {

    private Text key;
    private LongWritable value;

    LongEntry() {
        set(new Text(), new LongWritable());
    }

    LongEntry(String key, Long value) {
        set(new Text(key), new LongWritable(value));
    }

    Text getKey() {
        return key;
    }

    LongWritable getValue() {
        return value;
    }

    void setKey(String key) {
        this.key = new Text(key);
    }

    void setValue(Long value) {
        this.value = new LongWritable(value);
    }

    LongEntry copy() {
        return new LongEntry(key.toString(), value.get());
    }

    private void set(Text first, LongWritable second) {
        this.key = first;
        this.value = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        value.write(out);
    }

    @Override
    public String toString() {
        return key + " " + value;
    }

    @Override
    public int hashCode(){
        return key.hashCode()*163 + value.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof LongEntry)
        {
            LongEntry tp = (LongEntry) o;
            return key.equals(tp.key) && value.equals(tp.value);
        }
        return false;
    }
}
