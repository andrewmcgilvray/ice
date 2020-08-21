package com.netflix.ice.processor.postproc;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReservationService;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UserTag;

public class AllocationReport {
    Logger logger = LoggerFactory.getLogger(getClass());
	
	private DateTime start;
	private List<String> header;
	private List<Integer> inTagIndeces;
	private List<Integer> outTagIndeces;
	private List<Map<Key, List<Value>>> data;
	private List<String> inTagKeys;
	private List<String> outTagKeys;

	public AllocationReport(DateTime start, List<String> inTagKeys, List<String> outTagKeys, ResourceService resourceService) {
		this.start = start;
		this.header = Lists.newArrayList(new String[]{"Hour", "Allocation"});
		this.inTagIndeces = Lists.newArrayList();
		this.outTagIndeces = Lists.newArrayList();
		this.data = Lists.newArrayList();
		
		for (String key: inTagKeys) {
			inTagIndeces.add(resourceService.getUserTagIndex(key));
			header.add(key);
		}
		for (String key: outTagKeys) {
			outTagIndeces.add(resourceService.getUserTagIndex(key));
			header.add(key);
		}
		this.inTagKeys = inTagKeys;
		this.outTagKeys = outTagKeys;
	}
	
	public DateTime getStart() {
		return start;
	}
	
	public Key getKey(TagGroup tg) {
		List<String> inTags = Lists.newArrayListWithCapacity(32);
		for (String t: inTagKeys) {
			if (t.equals("Account"))
				inTags.add(tg.account.getId());
			else if (t.equals("Region"))
				inTags.add(tg.region.name);
			else if (t.equals("Zone"))
				inTags.add(tg.zone.name);
			else if (t.equals("Product"))
				inTags.add(tg.product.getServiceCode());
			else if (t.equals("Operation"))
				inTags.add(tg.operation.name);
			else if (t.equals("UsageType"))
				inTags.add(tg.usageType.name);
			else if (tg.resourceGroup != null) {
				UserTag[] userTags = tg.resourceGroup.getUserTags();
				for (int i = 0; i < inTagKeys.size(); i++) {
					if (t.equals(inTagKeys.get(i))) {
						inTags.add(userTags[inTagIndeces.get(i)].name);
						break;
					}
				}
			}
		}
		
		return new Key(inTags);
	}
	
	public List<Value> getData(int hour, Key key) {
		return data.get(hour).get(key);
	}
	
	public int getNumHours() {
		return data.size();
	}
	
	public Set<Key> getKeySet(int hour) {
		return data.get(hour).keySet();
	}
	
	public void add(int hour, double allocation, List<String> inTags, List<String> outTags) {
		while (data.size() <= hour) {
			data.add(Maps.<Key, List<Value>>newHashMap());
		}
		Map<Key, List<Value>> hourData = data.get(hour);
		
		Key k = new Key(inTags);
		List<Value> values = hourData.get(k);
		if (values == null) {
			values = Lists.newArrayList();
			hourData.put(k, values);
		}
		values.add(new Value(outTags, allocation));
	}
		
	public class Value {
		private final List<String> outputs;
		private final double allocation;
		
		public Value(List<String> outputs, double allocation) {
			this.outputs = outputs;
			this.allocation = allocation;
		}
		
		@Override
		public String toString() {
			return "{" + Double.toString(allocation) + ": " + outputs.toString() + "}";
		}
		
		public List<String> getOutputs() {
			return outputs;
		}
		
		public double getAllocation() {
			return allocation;
		}
		
		public TagGroup getOutputTagGroup(TagGroup tg) {
			UserTag[] userTags = tg.resourceGroup.getUserTags().clone();
			
			for (int i = 0; i < outputs.size(); i++) {
				String tag = outputs.get(i);
				if (tag.isEmpty())
					continue;
				
				userTags[outTagIndeces.get(i)] = UserTag.get(tag);
			}
			try {
				return tg.withResourceGroup(ResourceGroup.getResourceGroup(userTags));
			} catch (ResourceException e) {
				// should never throw because no user tags are null
				logger.error("error creating resource group from user tags: " + e);
			}
			return null;
		}
	}
	
	public static class Key {
		private List<String> inputs;
		private int hashcode;
		
		public Key(List<String> inputs) {
			this.inputs = inputs;
			this.hashcode = genHashCode();
		}
		
		@Override
		public String toString() {
			return inputs.toString();
		}
		
	    @Override
	    public int hashCode() {
	    	return hashcode;
	    }

	    @Override
	    public boolean equals(Object o) {
	    	if (this == o)
	    		return true;
	        if (o == null)
	            return false;
	        
	        Key other = (Key) o;
	        
	        for (int i = 0; i < inputs.size(); i++) {
	        	if (!inputs.get(i).equals(other.inputs.get(i)))
	        		return false;
	        }
	        return true;
	    }
	    
	    private int genHashCode() {
	        final int prime = 31;
	        int result = 1;
	        for (String t: inputs)
	        	result = prime * result + t.hashCode();

	        return result;
	    }
	    
	    public List<String> getInputs() {
	    	return inputs;
	    }
	}

    public void archive(String localDir, String bucket, String prefix, String filename) throws IOException {
        
        File file = new File(localDir, filename);
        
    	OutputStream os = new FileOutputStream(file);
		Writer out = new OutputStreamWriter(os);
        
        try {
        	writeCsv(out);
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(bucket, prefix, localDir, file.getName());
        logger.info("uploaded " + file);
    }
    
    private List<String> getValue(int hour, Key key, Value value) {
    	List<String> out = Lists.newArrayList();
    	out.add(start.plusHours(hour).toString());
    	out.add(Double.toString(value.getAllocation()));
    	out.addAll(key.getInputs());
    	out.addAll(value.getOutputs());
    	
    	return out;
    }

    public void writeCsv(Writer out) throws IOException {
    	String[] headerArray = new String[header.size()];
    	headerArray = header.toArray(headerArray);
    	
    	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(headerArray));
    	for (int hour = 0; hour < data.size(); hour++) {
    		Map<Key, List<Value>> hourData = data.get(hour);
    		for (Key k: hourData.keySet()) {
    			for (Value v: hourData.get(k)) {
    				printer.printRecord(getValue(hour, k, v));
    			}
    		}
    	}
  	
    	printer.close(true);
    }
    

    public void load(File file) {
        BufferedReader reader = null;
        try {
        	InputStream is = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(is));
            readCsv(reader);
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
    
    protected void readCsv(Reader reader) throws IOException {
    	data = Lists.newArrayList();
    	
    	Iterable<CSVRecord> records = CSVFormat.DEFAULT
    		      .withFirstRecordAsHeader()
    		      .parse(reader);
    	
	    for (CSVRecord record : records) {
	    	List<String> inTags = Lists.newArrayList();
	    	List<String> outTags = Lists.newArrayList();
	    	int hour = (int) ((new DateTime(record.get("Hour"), DateTimeZone.UTC).getMillis() - start.getMillis()) / (1000 * 60 * 60));
	    	double allocation = Double.parseDouble(record.get("Allocation"));
	    	for (String key: inTagKeys)
	    		inTags.add(record.get(key));
	    	for (String key: outTagKeys)
	    		outTags.add(record.get(key));
	    	add(hour, allocation, inTags, outTags);
	    }
    }
}
