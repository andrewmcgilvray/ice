package com.netflix.ice.processor.postproc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.config.S3BucketConfig;
import com.netflix.ice.tag.UserTag;

public class ReportWriter {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
	private String filename;
	private ReportConfig config;
	private String localDir;
	private DateTime month;
	boolean isCost;
	private List<String> header;
	private List<Rule.TagKey> tagKeys;
	private List<String> userTagKeys;
	private ReadWriteData data;
	private RuleConfig.Aggregation aggregation;
	
	public ReportWriter(String filename, ReportConfig config, String localDir,
			DateTime month, RuleConfig.DataType type,
			List<Rule.TagKey> tagKeys, List<String> userTagKeys,
			ReadWriteData data, RuleConfig.Aggregation aggregation) throws Exception {
		
		this.filename = filename;
		this.config = config;
		this.localDir = localDir;
		
		this.month = month;
		this.isCost = type == RuleConfig.DataType.cost;
		this.tagKeys = tagKeys;
		this.userTagKeys = userTagKeys;
		this.data = data;
		this.aggregation = aggregation;
		header = Lists.newArrayList();
		header.add("Date");
		header.add(isCost ? "Cost" : "Usage");
		if (type == RuleConfig.DataType.usage) {
			header.add("Units");
		}
		for (Rule.TagKey tk: tagKeys) {
			if (tk == Rule.TagKey.account) {
				header.add("Account ID");
				header.add("Account Name");
			}
			else
				header.add(tk.getColumnName());
		}
		header.addAll(userTagKeys);
	}

    public void archive() throws IOException {
    	File file = new File(localDir, filename);
    	OutputStream os = new FileOutputStream(file);
    	os = new GZIPOutputStream(os);
    	
		Writer out = new OutputStreamWriter(os);
        try {
        	writeCsv(out);
        }
        finally {
        	out.close();
        }
        
    	os.close();
    	
    	S3BucketConfig s3 = config.getS3Bucket();
    	String prefix = s3.getPrefix() == null ? "" : s3.getPrefix();
        logger.info(filename + " uploading to s3...");
        AwsUtils.upload(s3.getName(), s3.getRegion(), prefix, file, s3.getAccountId(), s3.getAccessRole(), s3.getExternalId());
        logger.info(filename + " uploading done.");    	

    }

    protected void writeCsv(Writer out) throws IOException {
    	String[] headerArray = new String[header.size()];
    	headerArray = header.toArray(headerArray);
        DateTimeFormatter isoFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC);
    	
    	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(headerArray));
    	    	
    	for (int index = 0; index < data.getNum(); index++) {
			DateTime date = month;
			if (index > 0) {
				switch (aggregation) {
				case monthly: date = date.plusMonths(index); break;
				case daily: date = date.plusDays(index); break;
				case hourly: date = date.plusHours(index); break;
				}
			}
			String dateString = date.toString(isoFormatter);
			
    		Map<TagGroup, Double> hourData = data.getData(index);
    		for (TagGroup tg: hourData.keySet()) {
    			Double v = hourData.get(tg);
    			
    			List<String> cols = Lists.newArrayList();
    			cols.add(dateString); // StartDate
    			cols.add(Double.toString(v));
    			if (!isCost)
    				cols.add(tg.usageType.unit);
    			
    			for (Rule.TagKey tk: tagKeys) {    				
    				switch (tk) {
    				case account:	cols.add(tg.account.getId()); cols.add(tg.account.getName()); break;
    				case region:	cols.add(tg.region.name); break;
    				case zone:		cols.add(tg.zone == null ? "" : tg.zone.name); break;
    				case product:	cols.add(tg.product.getServiceCode()); break;
    				case operation:	cols.add(tg.operation.name); break;
    				case usageType:	cols.add(tg.usageType.name); break;
    				default: break;
    				}    					
    			}
    			
    			if (!userTagKeys.isEmpty()) {
    				for (UserTag ut: tg.resourceGroup.getUserTags())
    					cols.add(ut.name);
    			}
    	    	printer.printRecord(cols);
    		}
    	}
    	printer.close(true);
    	
    }

}
