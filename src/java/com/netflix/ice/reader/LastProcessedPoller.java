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
package com.netflix.ice.reader;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.collect.Lists;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.Poller;
import com.netflix.ice.common.ProcessorStatus;

/**
 * LastProcessedPoller will periodically scan the timestamps from all the processorStatus_YYYY-MM files
 * to determine the latest timestamp from all the monthly files.
 */
public class LastProcessedPoller extends Poller {
	
    private final WorkBucketConfig workBucketConfig;
    private DateTime startDate;
    private DateTime lastProcessed;
    private List<ProcessorStatus> status;

	public LastProcessedPoller(DateTime startDate, WorkBucketConfig workBucketConfig) {
		this.startDate = startDate;
		this.workBucketConfig = workBucketConfig;
		this.lastProcessed = new DateTime(0);
		this.status = null;
		
		// Do the initial poll now
		try {
			poll();
		} catch (Exception e) {
			logger.error("Initial poll failed", e);
		}
		
		// Check every minute
		start(1*60, 1*60, false);
	}
	
	public Long getLastProcessedMillis() {
		return lastProcessed.getMillis();
	}

	@Override
	protected void poll() throws Exception {
        DateTime oldLastProcessed = lastProcessed;
		this.status = Lists.newArrayList();
        for (DateTime month = startDate; month.isBefore(DateTime.now()); month = month.plusMonths(1)) {
        	ProcessorStatus ps = getProcessorStatusForMonth(month);
        	if (ps == null)
        		continue;
        	
        	status.add(ps);
        	DateTime lastProcessedForMonth = ps.getLastProcessed();
        	if (lastProcessedForMonth.isAfter(lastProcessed))
        		lastProcessed = lastProcessedForMonth;
        }
        if (lastProcessed.isAfter(oldLastProcessed))
        	logger.info("Data updated at " + lastProcessed);
	}
	
    private ProcessorStatus getProcessorStatusForMonth(DateTime monthDate) {
    	String filename = ProcessorStatus.prefix + AwsUtils.monthDateFormat.print(monthDate) + ProcessorStatus.suffix;
    	
        AmazonS3Client s3Client = AwsUtils.getAmazonS3Client();
        InputStream in = null;
        try {
            in = s3Client.getObject(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix + filename).getObjectContent();
            ProcessorStatus ps = new ProcessorStatus(IOUtils.toString(in, StandardCharsets.UTF_8));
            return ps;
        }
        catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404)
            	logger.warn("File not found in s3: " + filename);
            else
            	logger.error("Error reading from file " + filename, e);
            return null;
        }
        catch (Exception e) {
            logger.error("Error reading from file " + filename, e);
            return null;
        }
        finally {
            if (in != null)
                try {in.close();} catch (Exception e){}
        }
    }
    
    public Collection<ProcessorStatus> getStatus() {
    	return status;
    }
}
