package Round1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Gram2 implements WritableComparable<Gram2> {

    private final Text w1;
    private final Text w2;
    private final IntWritable decade;

    public Gram2(Text w1, Text w2, IntWritable dec) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = new IntWritable((dec.get() / 10) * 10);
    }

    public Gram2(int dec) {
        this.w1 = new Text("*");
        this.w2 = new Text("N");
        this.decade = new IntWritable(dec);
    }

    public Gram2() {
        this(new Text("*"), new Text("*"), new IntWritable(-1));
    }

    public int compareTo(Gram2 other) {
        // Sort by decade and then by w1 and then by w2
        // if this.w1 == other.w1, put <w1, *> before <w1,w2>
        int res;

        if ((res = this.decade.get() - other.decade.get()) != 0)
            return res;
        else if (this.w1.toString().equals("*") && !other.w1.toString().equals("*"))
            return -1;
        else if (!this.w1.toString().equals("*") && other.w1.toString().equals("*"))
            return 1;
        else if ((res = this.w1.toString().compareTo(other.w1.toString())) != 0)
            return res;
        else
            return this.w2.toString().compareTo(other.w2.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        w1.write(dataOutput);
        w2.write(dataOutput);
        decade.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1.readFields(dataInput);
        w2.readFields(dataInput);
        decade.readFields(dataInput);
    }

    public IntWritable getDecade() {
        return this.decade;
    }

    public Text getW1() {
        return this.w1;
    }

    public Text getW2() {
        return this.w2;
    }

    @Override
    public String toString() {
        return this.decade + "-" + (this.decade.get() + 9) + " " + this.w1 + " " + this.w2;
    }

    public Text toText() {
        return new Text(this.toString());
    }
}
