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

import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.processor.ReadWriteDataSerializer.TagGroupFilter;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class DataWriter extends DataFile {
    protected ReadWriteDataSerializer data;

    DataWriter(String name, ReadWriteDataSerializer data, boolean load, WorkBucketConfig workBucketConfig,
    		AccountService accountService, ProductService productService) throws Exception {
    	super(name, workBucketConfig);
        this.data = data;

        if (!load)
        	return;
        
        AwsUtils.downloadFileIfNotExist(config.workS3BucketName, config.workS3BucketPrefix + file.getName(), file);

        if (file.exists()) {
        	InputStream is = new FileInputStream(file);
        	is = new GZIPInputStream(is);
            DataInputStream in = new DataInputStream(is);
            try {
                data.deserialize(accountService, productService, in);		
            }
            catch (Exception e) {
                throw new RuntimeException("DataWriter: failed to load " + file.getPath() + ", " + e + ", " + e.getMessage());
            }
            finally {
                in.close();
            }
        }
    }
    
	@Override
	protected void write(TagGroupFilter filter) throws IOException {
    	DataOutputStream out = new DataOutputStream(os);
        try {
        	data.serialize(out, filter);
    		out.flush();
        }
        finally {
        	out.close();
        }		
	}
}

