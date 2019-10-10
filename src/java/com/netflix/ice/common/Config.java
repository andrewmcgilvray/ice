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

import java.util.Map;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Config {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final String workBucketDataConfigFilename = "data_config.json";
    
    public final String workS3BucketName;
    public final String workS3BucketRegion;
    public final String workS3BucketPrefix;
    public final String localDir;
    public final ProductService productService;
    public final AWSCredentialsProvider credentialsProvider;
    public final Map<String, String> debugProperties;
    public final int numthreads;
    private TagCoverage tagCoverage;
    
    public enum TagCoverage {
    	none,
    	basic,
    	withUserTags   	
    }
    
    /**
     *
     * @param properties (required)
     * @param credentialsProvider (required)
     * @param accountService (required)
     * @param productService (required)
     * @param resourceService (optional)
     */
    public Config(
            Properties properties,
            AWSCredentialsProvider credentialsProvider,
            ProductService productService) {
        if (properties == null) throw new IllegalArgumentException("properties must be specified");
        if (productService == null) throw new IllegalArgumentException("productService must be specified");

        workS3BucketName = properties.getProperty(IceOptions.WORK_S3_BUCKET_NAME);
        workS3BucketRegion = properties.getProperty(IceOptions.WORK_S3_BUCKET_REGION);
        workS3BucketPrefix = properties.getProperty(IceOptions.WORK_S3_BUCKET_PREFIX);
        localDir = properties.getProperty(IceOptions.LOCAL_DIR);
        
        if (workS3BucketName == null) throw new IllegalArgumentException("IceOptions.WORK_S3_BUCKET_NAME must be specified");
        if (workS3BucketRegion == null) throw new IllegalArgumentException("IceOptions.WORK_S3_BUCKET_REGION must be specified");

        this.credentialsProvider = credentialsProvider;
        this.productService = productService;
        this.numthreads = properties.getProperty(IceOptions.PROCESSOR_THREADS) == null ? 5 : Integer.parseInt(properties.getProperty(IceOptions.PROCESSOR_THREADS));
        this.setTagCoverage(properties.getProperty(IceOptions.TAG_COVERAGE, "").isEmpty() ? TagCoverage.none : TagCoverage.valueOf(properties.getProperty(IceOptions.TAG_COVERAGE)));

        if (credentialsProvider != null)
        	AwsUtils.init(credentialsProvider, workS3BucketRegion);
        
        // Stash the arbitrary list of debug flags - names that start with "ice.debug."
        debugProperties = Maps.newHashMap();
        for (String name: properties.stringPropertyNames()) {
        	if (name.startsWith(IceOptions.DEBUG)) {
				String key = name.substring(IceOptions.DEBUG.length() + 1);
        		debugProperties.put(key, properties.getProperty(name));
        	}
        }
    }

	public TagCoverage getTagCoverage() {
		return tagCoverage;
	}
	
	public boolean hasTagCoverage() {
		return tagCoverage != TagCoverage.none;
	}
	
	public boolean hasTagCoverageWithUserTags() {
		return tagCoverage == TagCoverage.withUserTags;
	}

	protected void setTagCoverage(TagCoverage tagCoverage) {
		this.tagCoverage = tagCoverage;
	}
    
}
