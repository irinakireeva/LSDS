
package upf.edu.uploader;

import java.util.List;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.regions.Regions;
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
            AmazonS3 S3Client = AmazonS3ClientBuilder
                                .standard()
                                .withCredentials(new ProfileCredentialsProvider(this.profileName))
                                .withRegion(Regions.US_EAST_1)
                                .build();

            if(!S3Client.doesBucketExist(this.bucketName)) {
                System.out.println("This bucket does not exist.");
                return;
                
            }            
    
            for(String file : files){
                S3Client.putObject(this.bucketName,file, (prefix + file));
            }
            
        } catch (AmazonServiceException e) {
            return;
        }
    }
}