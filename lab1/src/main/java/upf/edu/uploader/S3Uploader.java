package upf.edu.uploader;

import java.util.List;
import com.amazonaws.auth;
import com.amazonaws.services.s3.AmazonS3;
import software.amazon.awssdk.regions.Region;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;


public class S3Uploader implements Uploader {
    private final String bucketName;
    private final String prefix;
    private final String profileName;


    public S3Uploader(String bucketName, String prefix, String profileName) {
        this.bucketName = bucketName;
        this.prefix = prefix;
        this.profileName = profileName;
    }

    @Override
    public void upload(List<String> files){
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder
                                .standard()
                                .withCredentials(ProfileCredentialsProvider(this.profileName))
                                .withRegion(Region.US_EAST_1)
                                .build();

            if(s3client.doesBucketExist(this.bucketName)) {
                System.out.println("This bucket already exists.");
                return;
                
            }                                
            s3client.createBucket(bucketName);
            for(String file : files){
                s3client.putObject(this.bucketName,file, new File(prefix + file));
            }
            
        } catch (AmazonServiceException e) {
            return;
        }
    }
}