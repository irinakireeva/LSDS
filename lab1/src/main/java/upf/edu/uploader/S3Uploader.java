package upf.edu.uploader;

import java.util.List;

public class S3Uploader {
    private final String Bucket;
    private final String Prefix;
    private final String UPF;

    public S3Uploader(String bucket, String prefix, String upf) {
        Bucket = bucket;
        Prefix = prefix;
        UPF = upf;
    }

    public void upload(List<String> asList) {
    }
}
