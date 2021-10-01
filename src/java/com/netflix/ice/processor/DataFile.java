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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.processor.ReadWriteDataSerializer.TagGroupFilter;

public abstract class DataFile {
    private final static Logger logger = LoggerFactory.getLogger(DataWriter.class);

    private static final String compressExtension = ".gz";
    
    protected final WorkBucketConfig config;
    protected final String dbName;
    protected final File file;
    
    protected OutputStream os;

    protected DataFile(String name, WorkBucketConfig config) throws Exception {
    	this.config = config;
        dbName = name;
        os = null;
        
        String filename = dbName + compressExtension;
        file = config == null ? new File(filename) : new File(config.localDir, filename);
    }
    
    // For unit testing
    protected DataFile() {
    	config = null;
    	dbName = null;
    	file = null;
    }
    
    public void open() throws IOException {
    	os = new FileOutputStream(file);
    	os = new GZIPOutputStream(os);
    }
    
    public void close() throws IOException {
    	os.close();
    	
        logger.info(this.dbName + " uploading to s3...");
        AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix + file.getName(), file);
        logger.info(this.dbName + " uploading done.");    	
    }
    
    public void archive() throws IOException {
    	archive(null);
    }
    
    public void archive(TagGroupFilter filter) throws IOException {
    	open();
    	write(filter);
    	close();
    }
    
    public void delete() {
    	file.delete();
    }

    abstract protected void write(TagGroupFilter filter) throws IOException;
}
