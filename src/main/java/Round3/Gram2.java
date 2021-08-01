package Round3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class Gram2 implements WritableComparable<Gram2> {

    private final Text w1;
    private final Text w2;
    private final IntWritable decade;
    private final DoubleWritable npmi;

    public Gram2() {
        w1 = new Text("*");
        w2 = new Text("*");
        decade = new IntWritable(0);
        this.npmi = new DoubleWritable(0);
    }

    public Gram2(Text w1, Text w2, int dec, double npmi) {
        this.w1 = w1;
        this.w2 = w2;
        this.decade = new IntWritable(dec);
        this.npmi = new DoubleWritable(npmi);
    }

    public Gram2(int dec) {
        this.w1 = new Text("*");
        this.w2 = new Text("*");
        this.decade = new IntWritable(dec);
        this.npmi = new DoubleWritable(0);
    }

    @Override
    public int compareTo(Gram2 other) {
        // dec * * is always "smaller"
        // Sort by decade, then npmi, then w1 and then w2
        int res;

        if ((res = this.getDecade().get() - other.getDecade().get()) != 0)      // Decade
            return res;

        // decade * *
        if (this.isRelativePMI() && !other.isRelativePMI())
            return -1;
        if (!this.isRelativePMI() && other.isRelativePMI())
            return 1;

        if (this.getNpmi().get() - other.getNpmi().get() != 0)             // npmi
            return this.getNpmi().get() < other.getNpmi().get() ? 1 : -1;

        if ((res = this.getW1().compareTo(other.getW1())) != 0)             // W1
            return res;

        return this.getW2().compareTo(other.getW2());                       // W2
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        w1.write(dataOutput);
        w2.write(dataOutput);
        decade.write(dataOutput);
        npmi.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1.readFields(dataInput);
        w2.readFields(dataInput);
        decade.readFields(dataInput);
        npmi.readFields(dataInput);
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

    public DoubleWritable getNpmi() {
        return this.npmi;
    }

    public boolean isRelativePMI() {
        return this.w1.toString().equals("*") && this.w2.toString().equals("*");
    }

    @Override
    public String toString() {
        return this.decade + " " + this.w1 + " " + this.w2;
    }

    public Text toText() {

        return new Text(this.decade + "-" + (this.decade.get() + 9) + "\t\t" +
                "nmpi = " + round(this.npmi.get(), 5) + "\t\t" +
                this.w1 + " " +
                this.w2
        );
    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
