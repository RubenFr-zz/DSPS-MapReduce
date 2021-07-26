package Round1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Gram2 implements WritableComparable<Gram2> {

    private Text w1;
    private Text w2;
    private IntWritable decade;

    public Gram2(Text w1, Text w2, IntWritable dec) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = new IntWritable((dec.get() / 10) * 10);
    }

    public Gram2(Text w1, Text w2) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = new IntWritable(-1);
    }

    public Gram2(int dec) {
        this.w1 = new Text("*");
        this.w2 = new Text("*");
        this.decade = new IntWritable(dec);
    }

    public Gram2() {
        this(new Text("^"), new Text("^"), new IntWritable(-1));
    }

    public int compareTo(Gram2 other) {
        // N is always "smaller"
        // Sort by decade and then by w1 and then by w2
        // if this.w1 == other.w1, put <w1, *> before <w1,w2>
        int res;

        if ((res = this.decade.get() - other.decade.get()) != 0)
            return res;
        else if (this.is_N() && !other.is_N())
            return -1;
        else if (!this.is_N() && other.is_N())
            return 1;
        else if ((res = this.w1.toString().compareTo(other.w1.toString())) != 0)
            return res;
        else if (this.w2.toString().equals("*") && !other.w2.toString().equals("*"))
            return -1;
        else if (!this.w2.toString().equals("*") && other.w2.toString().equals("*"))
            return 1;
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

    public String getW1() {
        return this.w1.toString();
    }

    public String getW2() {
        return this.w2.toString();
    }

    public boolean is_N() {
        return this.w1.toString().equals("*") && this.w2.toString().equals("*");
    }

    public Gram2 setDecade(int dec) {
        this.decade = new IntWritable(dec);
        return this;
    }

    public Gram2 setDecade(IntWritable dec) {
        this.decade = dec;
        return this;
    }

    public void setW1(Text new_w1) {
        this.w1 = new_w1;
    }

    public void setW2(Text new_w2) {
        this.w2 = new_w2;
    }

    @Override
    public String toString() {
        return this.decade + "-" + (this.decade.get() + 9) + " " + this.w1 + " " + this.w2;
    }

    public Text toText() {
        return new Text(this.toString());
    }
}
