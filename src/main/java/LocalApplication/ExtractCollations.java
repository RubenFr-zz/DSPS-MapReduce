package LocalApplication;

import java.io.*;

import awsService.EMRService;
import awsService.StorageService;
import software.amazon.awssdk.services.emr.model.*;

public class ExtractCollations {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java -jar Round1.jar <input> <output> <minPmi> <relMinPmi>\n");
            System.exit(-1);
        }

        String minPmi = args[0];
        String relMinPmi = args[1];

        StorageService s3 = new StorageService(Constants.BUCKET_NAME);

        // 1. Upload jars to S3
        s3.uploadFile("Target/Round1/Round1.jar", "Round1.jar");
//        s3.uploadFile("Target/Round1/test.txt", "input.txt");
//        s3.uploadFile("Target/Round2/Round2.jar", "Round2.jar");

        // 2. Create Config for Round 1
        HadoopJarStepConfig hadoopJarRound1 = HadoopJarStepConfig.builder()
                .jar("s3n://" + Constants.BUCKET_NAME + "/Round1.jar")
                .mainClass("Round1")
                .args(Constants.gram2_address, "s3://" + Constants.BUCKET_NAME + "/output_Round1")
//                .args("s3://" + Constants.BUCKET_NAME + "/input.txt", "s3://" + Constants.BUCKET_NAME + "/output_Round1")
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
                .args("s3://" + Constants.BUCKET_NAME + "/output_Round1", "s3://" + Constants.BUCKET_NAME + "/output_Round2", minPmi, relMinPmi)
                .build();

        StepConfig stepConfigRound2 = StepConfig.builder()
                .name("Round 2")
                .hadoopJarStep(hadoopJarRound2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // 4. Run Application
        EMRService emr = new EMRService();

//        String jobFlowId = emr.runApplication(stepConfigRound1, stepConfigRound2);
        String jobFlowId = emr.runApplication(stepConfigRound1);
        System.out.println("Ran job flow with id: " + jobFlowId);

//        find_min_max_years("C:\\Users\\Ruben\\Documents\\Workspace\\Distributed Systems\\DSPS-MapReduce\\Target\\Round1\\input.txt");
//        add_double_quotes("src/main/java/LocalApplication/test.txt", "src/main/java/LocalApplication/stopWords.txt");'
//        readlines("C:\\Users\\Ruben\\Documents\\Workspace\\Distributed Systems\\DSPS-MapReduce\\Target\\Round1\\heb-2gram.txt");
//        readlines("C:\\Users\\Ruben\\Documents\\Workspace\\Distributed Systems\\DSPS-MapReduce\\Target\\Round1\\output_after\\.part-r-00000.crc");
    }

    private static void readlines(String inputFile) {
        BufferedReader reader;
        int i = 0;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            String line;

            while ((line = reader.readLine()) != null && i < 100) {
                System.out.println(line);
                i++;
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void add_double_quotes(String inputFile, String outputFile) {
        BufferedReader reader;
        FileWriter writer;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            writer = new FileWriter(outputFile);
            String line;

            while ((line = reader.readLine()) != null) {
                writer.write("\"" + line + "\", ");
            }
            writer.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void find_min_max_years(String inputFile) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            String line;

            while ((line = reader.readLine()) != null) {
                int curr = Integer.parseInt(line.split("\t")[1]);
                min = Math.min(min, curr);
                max = Math.max(max, curr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Min = " + min);
        System.out.println("Max = " + max);
    }
}
