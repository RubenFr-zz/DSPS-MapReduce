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
    private final IntWritable type;   // 0: Not Defined, 1: w1 *, 2: * w2

    public Gram2() {
        w1 = new Text("*");
        w2 = new Text("*");
        decade = new IntWritable(-1);
        type = new IntWritable(0);
    }

    public Gram2(int dec, int type) {
        w1 = new Text("*");
        w2 = new Text("*");
        decade = new IntWritable(dec);
        this.type = new IntWritable(type);
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
        // Sort according to type:
        //      Type 1: sort by * then w1 then decade then w2
        //      Type 2: sort by * then w2 then decade then w1
        // Sort by decade and then w1 then w2

        // N
        if (this.is_N() && !other.is_N())
            return -1;
        if (!this.is_N() && other.is_N())
            return 1;

        // Not same type (not supposed to happen)
        if (this.getType() != other.getType())
            return this.getType() - other.getType();

        // Type 1 or Type 2
        if (this.getType() == 1)
            return this.compareToType1(other);
        if (this.getType() == 2)
            return this.compareToType2(other);

        return 0;

    }

    // 0 w1 * or dec w1 w2
    private int compareToType1(Gram2 other) {
        int res;

        if ((res = this.getW1().compareTo(other.getW1())) != 0)
            return res;

        if (this.isW2Star() && !other.isW2Star())
            return -1;
        if (!this.isW2Star() && other.isW2Star())
            return 1;

        if ((res = this.getDecade() - other.getDecade()) != 0)
            return res;
        return this.getW2().compareTo(other.getW2());
    }

    // 0 * w2 or dec w1 w2
    private int compareToType2(Gram2 other) {
        int res;

        if ((res = this.getW2().compareTo(other.getW2())) != 0)
            return res;

        if (this.isW1Star() && !other.isW1Star())
            return -1;
        if (!this.isW1Star() && other.isW1Star())
            return 1;

        if ((res = this.getDecade() - other.getDecade()) != 0)
            return res;
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
        if (this.is_N())
            return this.decade.get();

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

    public boolean isW1Star() {
        return this.getW1().equals("*");
    }

    public boolean isW2Star() {
        return this.getW2().equals("*");
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
