package LocalApplication;

import awsService.EMRService;
import awsService.StorageService;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.StepConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ExtractCollations {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java -jar Local.jar <minPmi> <relMinPmi>\n");
            System.exit(-1);
        }

        String minPmi = args[0];
        String relMinPmi = args[1];

        StorageService s3 = new StorageService(Constants.BUCKET_NAME);

        // 1. Upload jars to S3
        s3.uploadFile("target/Round1/Round1.jar", "Round1.jar");
        s3.uploadFile("target/Round2/Round2.jar", "Round2.jar");
        s3.uploadFile("target/Round3/Round3.jar", "Round3.jar");

        // 2. Create Config for Round 1
        HadoopJarStepConfig hadoopJarRound1 = HadoopJarStepConfig.builder()
                .jar("s3n://" + Constants.BUCKET_NAME + "/Round1.jar")
                .mainClass("Round1")
                .args(Constants.gram2_address, "s3://" + Constants.BUCKET_NAME + "/output_r1")
                .build();

        StepConfig stepConfigRound1 = StepConfig.builder()
                .name("Round 1")
                .hadoopJarStep(hadoopJarRound1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // 3. Create Config for Round 2
        HadoopJarStepConfig hadoopJarRound2 = HadoopJarStepConfig.builder()
                .jar("s3n://" + Constants.BUCKET_NAME + "/Round2.jar")
                .mainClass("Round2")
                .args("s3://" + Constants.BUCKET_NAME + "/output_r1", "s3://" + Constants.BUCKET_NAME + "/output_r2")
                .build();

        StepConfig stepConfigRound2 = StepConfig.builder()
                .name("Round 2")
                .hadoopJarStep(hadoopJarRound2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // 4. Create Config for Round 3
        HadoopJarStepConfig hadoopJarRound3 = HadoopJarStepConfig.builder()
                .jar("s3n://" + Constants.BUCKET_NAME + "/Round3.jar")
                .mainClass("Round3")
                .args("s3://" + Constants.BUCKET_NAME + "/output_r2",
                        "s3://" + Constants.BUCKET_NAME + "/output_r3",
                        minPmi, relMinPmi)
                .build();

        StepConfig stepConfigRound3 = StepConfig.builder()
                .name("Round 3")
                .hadoopJarStep(hadoopJarRound3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // 5. Run Application
        EMRService emr = new EMRService();

        String jobFlowId = emr.runApplication(stepConfigRound1, stepConfigRound2, stepConfigRound3);
        System.out.println("Ran job flow with id: " + jobFlowId);
}
}
