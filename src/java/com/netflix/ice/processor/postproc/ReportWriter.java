package com.netflix.ice.processor.postproc;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.Lists;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.DataFile;
import com.netflix.ice.processor.ReadWriteData;
import com.netflix.ice.processor.ReadWriteDataSerializer.TagGroupFilter;
import com.netflix.ice.tag.UserTag;

public class ReportWriter extends DataFile {
	private DateTime month;
	boolean isCost;
	private List<String> header;
	private List<Rule.TagKey> tagKeys;
	private List<String> userTagKeys;
	private ReadWriteData data;
	
	public ReportWriter(String name, WorkBucketConfig config,
			DateTime month, RuleConfig.DataType type,
			List<Rule.TagKey> tagKeys, List<String> userTagKeys,
			ReadWriteData data) throws Exception {
		
		super(name, config);
		
		this.month = month;
		this.isCost = type == RuleConfig.DataType.cost;
		this.tagKeys = tagKeys;
		this.userTagKeys = userTagKeys;
		this.data = data;
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
				header.add(tk.toString());
		}
		header.addAll(userTagKeys);
	}
	
	@Override
	protected void write(TagGroupFilter filter) throws IOException {
		Writer out = new OutputStreamWriter(os);
        try {
        	writeCsv(out);
        }
        finally {
        	out.close();
        }		
	}

    protected void writeCsv(Writer out) throws IOException {
    	String[] headerArray = new String[header.size()];
    	headerArray = header.toArray(headerArray);
        DateTimeFormatter isoFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC);
    	
    	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(headerArray));
    	
    	for (int hour = 0; hour < data.getNum(); hour++) {
    		Map<TagGroup, Double> hourData = data.getData(hour);
    		for (TagGroup tg: hourData.keySet()) {
    			Double v = hourData.get(tg);
    			
    			List<String> cols = Lists.newArrayList();
    			cols.add(month.plusHours(hour).toString(isoFormatter)); // StartDate
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
