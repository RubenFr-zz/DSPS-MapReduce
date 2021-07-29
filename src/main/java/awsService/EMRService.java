package awsService;

import LocalApplication.Constants;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class EMRService {

    private final EmrClient emr;

    public static String access_key_id = "ASIA26O7V7QIM4YJGUGO";
    public static String secret_access_key_id = "1W54ZYOLyo0f0Wlfq+Uw1I44Ndld+mzrzuAq7efu";
    public static String aws_session_token = "FwoGZXIvYXdzEHsaDIztxwiKgB0CvZkiyCLFAY9FjPa8o4cJ1xVs71wTouL05IBO48ZWKh/rYxZ+urqMa/4iuHfJrJtwCcIuGm1xXBoWmtnmUoIOheKrV1DVl9m4mIgkXnzaheIY85lXINepulOytJwvbV/dwjscOYU7YQoO12LZ8eMr2m0EdEJAoHLUauXMEZN/mbwPn4VZB+ipNedpaQNwkDYd1IJcScaGP9wHgeQS+Bo4AbHGDVvExToUETEWSUR3PQ3wIHUpXNBU8kgOZglaNsRcZdPMugM192gZIH4KKOTJhIgGMi0Mjrm9u6gCK7Rk2GPXWal2UvUlDapgxeYS4VQBu2dLNWg4Z7oAnZPHyAK8Y24=";


    public EMRService() {

//        AwsSessionCredentials awsCreds = AwsSessionCredentials.create(access_key_id, secret_access_key_id, aws_session_token);
//
//        AwsCredentialsProvider credentials =
//                ProfileCredentialsProvider.builder()
//                        .profileName("default")
//                        .build();

        emr = EmrClient.builder()
                .region(Region.US_EAST_1)   // Region to create/load the instance
//                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
    }

    public String runApplication(StepConfig... steps) {

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(5)
                .masterInstanceType(InstanceType.M4_XLARGE.toString())
                .slaveInstanceType(InstanceType.M4_XLARGE.toString())
                .hadoopVersion("3.2.1")
                .ec2KeyName("ec2-java-ssh")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .build();

        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("2Gram-Map-Reduce")
                .instances(instances)
                .steps(steps)
                .logUri("s3n://" + Constants.BUCKET_NAME + "/log/")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-6.2.0")  // Hadoop 3.2.1
                .build();

        RunJobFlowResponse response = emr.runJobFlow(request);
        return response.jobFlowId();
    }
}
