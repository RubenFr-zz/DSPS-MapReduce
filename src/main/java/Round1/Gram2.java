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
    private IntWritable decade;
    private final IntWritable type;     // 0: W1    1: W2

    public Gram2() {
        w1 = new Text("*");
        w2 = new Text("*");
        decade = new IntWritable(-1);
        type = new IntWritable(-1);
    }

    public Gram2(Text w1, Text w2, IntWritable dec, int type) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = new IntWritable((dec.get() / 10) * 10);
        this.type = new IntWritable(type);
    }

    public Gram2(Text w1, Text w2, int type) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = new IntWritable(0);
        this.type = new IntWritable(type);
    }

    @Override
    public int compareTo(Gram2 other) {
        // N is always "smaller"
        // Sort by decade and then w1 < w2 or w2 < w1
        // if this.w1 == other.w1, put <w1, *> before <w1,w2>
        int res;

        if (this.is_N() && !other.is_N())      // N
            return -1;
        else if (!this.is_N() && other.is_N())      // N
            return 1;
        else if ((res = this.getDecade() - other.getDecade()) != 0)
            return res;

        else if (this.getType() == 0)
            return this.compareTo1(other);
        else
            return this.compareTo2(other);
    }

    public int compareTo1(Gram2 other) {
        int res;

        if ((res = this.getW1().compareTo(other.getW1())) != 0)
            return res;
        else if (this.getW2().equals("*") && !other.getW2().equals("*"))
            return -1;
        else if (!this.getW2().equals("*") && other.getW2().equals("*"))
            return 1;
        else
            return this.getW2().compareTo(other.getW2());
    }

    public int compareTo2(Gram2 other) {
        int res;

        if ((res = this.getW2().compareTo(other.getW2())) != 0)
            return res;
        else if (this.getW1().equals("*") && !other.getW1().equals("*"))
            return -1;
        else if (!this.getW1().equals("*") && other.getW1().equals("*"))
            return 1;
        else
            return this.getW1().compareTo(other.getW1());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        w1.write(dataOutput);
        w2.write(dataOutput);
        decade.write(dataOutput);
        type.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1.readFields(dataInput);
        w2.readFields(dataInput);
        decade.readFields(dataInput);
        type.readFields(dataInput);
    }

    public int getDecade() {
        return this.decade.get() / 10;
    }

    public String getW1() {
        return this.w1.toString();
    }

    public String getW2() {
        return this.w2.toString();
    }

    public int getType() {
        return this.type.get();
    }

    public Gram2 setType(int type) {
        return new Gram2(this.w1, this.w2, this.decade, type);
    }

    public void setDecade(int dec) {
        this.decade = new IntWritable(dec);
    }

    public boolean is_N() {
        return this.w1.toString().equals("*") && this.w2.toString().equals("*");
    }

    @Override
    public String toString() {
        return this.decade + " " + this.w1 + " " + this.w2;
    }

    public Text toText() {
        return new Text(this.toString());
    }


}
