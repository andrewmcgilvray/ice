package com.netflix.ice.common;

public class WorkBucketConfig {
    public final String workS3BucketName;
    public final String workS3BucketRegion;
    public final String workS3BucketPrefix;
    public final String localDir;

    public WorkBucketConfig(String workS3BucketName, String workS3BucketRegion, String workS3BucketPrefix, String localDir) {
        this.workS3BucketName = workS3BucketName;
        this.workS3BucketRegion = workS3BucketRegion;
        this.workS3BucketPrefix = workS3BucketPrefix;
        this.localDir = localDir;
    }
}
