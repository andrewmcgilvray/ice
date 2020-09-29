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
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTag;

public class ReportWriter extends DataFile {
	private DateTime month;
	boolean isCost;
	private List<String> header;
	private List<TagType> tagKeys;
	private List<Integer> userTagKeyIndeces;
	private ReadWriteData data;
	
	public ReportWriter(String name, WorkBucketConfig config,
			DateTime month, OperandConfig.OperandType type,
			List<TagType> tagKeys, List<Integer> userTagKeyIndeces, List<String> userTagKeys,
			ReadWriteData data) throws Exception {
		
		super(name, config);
		
		this.month = month;
		this.isCost = type == OperandConfig.OperandType.cost;
		this.tagKeys = tagKeys;
		this.userTagKeyIndeces = userTagKeyIndeces;
		this.data = data;
		header = Lists.newArrayList();
		header.add("Date");
		header.add(isCost ? "Cost" : "Usage");
		if (type == OperandConfig.OperandType.usage) {
			header.add("Units");
		}
		for (TagType tt: tagKeys) {
			if (tt == TagType.Account) {
				header.add("Account ID");
				header.add("Account Name");
			}
			else
				header.add(tt.toString());
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

    			for (TagType tt: tagKeys) {    				
    				switch (tt) {
    				case Account:	cols.add(tg.account.getId()); cols.add(tg.account.getName()); break;
    				case Region:	cols.add(tg.region.name); break;
    				case Zone:		cols.add(tg.zone == null ? "" : tg.zone.name); break;
    				case Product:	cols.add(tg.product.getServiceCode()); break;
    				case Operation:	cols.add(tg.operation.name); break;
    				case UsageType:	cols.add(tg.usageType.name); break;
    				default: break;
    				}    					
    			}
    			
    			if (!userTagKeyIndeces.isEmpty()) {
    				UserTag[] userTags = tg.resourceGroup.getUserTags();
	    			for (Integer i: userTagKeyIndeces) {
	    				cols.add(userTags[i].name);
	    			}
    			}
    	    	printer.printRecord(cols);
    		}
    	}
    	printer.close(true);
    	
    }

}
