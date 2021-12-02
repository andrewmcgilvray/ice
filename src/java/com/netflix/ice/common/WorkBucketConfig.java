/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
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
