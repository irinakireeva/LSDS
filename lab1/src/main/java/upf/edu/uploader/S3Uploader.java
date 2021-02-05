package upf.edu.uploader;

import java.util.List;

public class S3Uploader {
    private final String BucketName;
    private final String Prefix;
    private final String Credentials;

    public S3Uploader(String bucket, String prefix, String upf) {
        BucketName = bucket;
        Prefix = prefix;
        Credentials = upf;
    }

    public void upload(List<String> asList) {

    }
}
