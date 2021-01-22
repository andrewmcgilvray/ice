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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Instance;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;
import com.netflix.ice.tag.Zone.BadZone;

public class Instances {
    private final static Logger logger = LoggerFactory.getLogger(Instances.class);
    
	private final String localDir;
	private final String workS3BucketName;
	private final String workS3BucketPrefix;
	private ConcurrentMap<String, Instance> data;

	public Instances(String localDir, String workS3BucketName, String workS3BucketPrefix) {
    	this.localDir = localDir;
		this.workS3BucketName = workS3BucketName;
		this.workS3BucketPrefix = workS3BucketPrefix;
		data = Maps.newConcurrentMap();
	}
	
	public void add(String id, long startMillis, String type, Map<String, String> tags, Account account, Region region, Zone zone, Product product) {
		if (id.isEmpty()) {
			return;
		}
		// Save the most recent version of the resource data.
		Instance i = data.get(id);
		if (i == null || i.startMillis < startMillis)
			data.put(id, new Instance(id, type, account, region, zone, product, tags, startMillis));
	}
	
	public Instance get(String id) {
		return data.get(id);
	}
	
	public Collection<Instance> values() {
		return data.values();
	}
	
	public int size() {
		return data.size();
	}
	
	private String getFilename(long timeMillis) {
        DateTime monthDateTime = new DateTime(timeMillis, DateTimeZone.UTC);
		return "instances_" + AwsUtils.monthDateFormat.print(monthDateTime) + ".csv.gz";
	}

    public void archive(long timeMillis) throws IOException {
        
        File file = new File(localDir, getFilename(timeMillis));
        
    	OutputStream os = new FileOutputStream(file);
		os = new GZIPOutputStream(os);        
		Writer out = new OutputStreamWriter(os);
        
        try {
        	writeCsv(out);
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(workS3BucketName, workS3BucketPrefix, file);
        logger.info("uploaded " + file);
    }
    
    protected void writeCsv(Writer out) throws IOException {
    	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(Instance.header()));
    	for (Instance instance: data.values()) {
    		printer.printRecord((Object[]) instance.values());
    	}
  	
    	printer.close(true);
    }
    
    public void retrieve(long timeMillis, AccountService accountService, ProductService productService) {
        File file = new File(localDir, getFilename(timeMillis));
    	
        // read from s3 if not exists
        boolean downloaded = false;
        
        try {
            downloaded = AwsUtils.downloadFileIfChanged(workS3BucketName, workS3BucketPrefix, file);
        }
        catch (Exception e) {
            logger.error("error downloading " + file, e);
            return;
        }
        if (downloaded || (data.size() == 0 && file.exists())) {
            BufferedReader reader = null;
            try {
            	InputStream is = new FileInputStream(file);
            	is = new GZIPInputStream(is);
                reader = new BufferedReader(new InputStreamReader(is));
                readCsv(reader, accountService, productService);
            }
            catch (Exception e) {
            	Logger logger = LoggerFactory.getLogger(ReservationService.class);
            	logger.error("error in reading " + file, e);
            }
            finally {
                if (reader != null)
                    try {reader.close();} catch (Exception e) {}
            }
        }        
    }
    
    protected void readCsv(Reader reader, AccountService accountService, ProductService productService) throws IOException, BadZone {
    	int numCols = Instance.header().length;
    	
    	Iterable<CSVRecord> records = CSVFormat.DEFAULT
    		      .withHeader(Instance.header())
    		      .withFirstRecordAsHeader()
    		      .parse(reader);
    	
    	ConcurrentMap<String, Instance> dataMap = Maps.newConcurrentMap();
        String[] values = new String[numCols];
	    for (CSVRecord record : records) {
	    	for (int i = 0; i < numCols; i++)
	    		values[i] = record.get(i);
	    	
        	Instance instance = new Instance(values, accountService, productService);
        	dataMap.put(instance.id, instance);

	    }
	    data = dataMap;
    }
}
