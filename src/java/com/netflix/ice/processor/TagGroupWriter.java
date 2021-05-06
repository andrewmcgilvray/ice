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
package com.netflix.ice.processor;

import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TagGroupWriter {
    private final static Logger logger = LoggerFactory.getLogger(TagGroupWriter.class);
    public final static String DB_PREFIX = "tagdb_";
    private static final String compressExtension = ".gz";

    private TreeMap<Long, Collection<TagGroup>> tagGroups;
    private final String dbName;
    private final File file;
    private final boolean compress;
    private final WorkBucketConfig workBucketConfig;

    TagGroupWriter(String name, boolean compress, WorkBucketConfig workBucketConfig, AccountService accountService, ProductService productService, int numUserTags) throws Exception {
    	this.compress = compress;
    	this.workBucketConfig = workBucketConfig;

        dbName = DB_PREFIX + name;
        String filename = dbName + (compress ? compressExtension : "");
        file = new File(workBucketConfig.localDir, filename);
        logger.info("creating TagGroupWriter for " + file);
        AwsUtils.downloadFileIfNotExist(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, file);

        if (file.exists()) {
        	InputStream is = new FileInputStream(file);
        	if (compress)
        		is = new GZIPInputStream(is);
            DataInputStream in = new DataInputStream(is);
            try {
                tagGroups = TagGroup.Serializer.deserializeTagGroups(accountService, productService, numUserTags, in);
            }
            finally {
                if (in != null)
                    in.close();
            }
        }
        else {
            tagGroups = Maps.newTreeMap();
        }
    }

    void archive(Long monthMilli,Collection<TagGroup> tagGroups) throws IOException {
        this.tagGroups.put(monthMilli, tagGroups);

        OutputStream os = new FileOutputStream(file);
    	if (compress)
    		os = new GZIPOutputStream(os);
    	DataOutputStream out = new DataOutputStream(os);
        try {
            TagGroup.Serializer.serializeTagGroups(out, this.tagGroups);
            out.flush();
        }
        finally {
            out.close();
        }
        
        logger.info(dbName + " uploading to s3...");
        AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, file);
        logger.info(dbName + " uploading done.");
    }
    
    // Output file to CSV for general debugging
    void outputCsv(String dir) throws IOException {
    	new File(dir).mkdirs();
        File csvFile = new File(dir, dbName + ".csv");
        DataOutputStream out = new DataOutputStream(new FileOutputStream(csvFile));
        try {
            TagGroup.Serializer.serializeTagGroupsCsv(out, this.tagGroups);
            out.flush();
        }
        finally {
            out.close();
        }
	}
}

