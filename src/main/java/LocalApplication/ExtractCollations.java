package LocalApplication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.model.*;

public class ExtractCollations {
    private static final String DATA_URL = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    private static final String BUCKET_NAME = "dpsp-hadoop";

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Error: 2 arguments required, found " + args.length);
            System.exit(-1);
        }
        String minPmi = args[0];
        String relMinPmi = args[1];

        HadoopJarStepConfig hadoopJar = HadoopJarStepConfig.builder()
                .jar("s3n://" + BUCKET_NAME + "/task2.jar")
                .mainClass("XXX") // TODO
                .args(minPmi, relMinPmi, "s3://" + BUCKET_NAME + "/output")
                .build();

        StepConfig stepConfig = StepConfig.builder()
                .name("task2")
                .hadoopJarStep(hadoopJar)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
    }
}
