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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.TagMappings;
import com.netflix.ice.processor.Report;
import com.netflix.ice.processor.config.S3BucketConfig;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UserTag;

public class AllocationReport extends Report {
    Logger logger = LoggerFactory.getLogger(getClass());
    
    enum AllocationColumn {
    	StartDate,
    	EndDate,
    	Allocation;
    }
	
	private AllocationConfig config;
	private List<String> inTagKeys;
	private List<String> outTagKeys;
	private List<Integer> inTagIndeces; // ResourceGroup user tag indeces for the input tags
	private List<Integer> outTagIndeces; // ResourceGroup user tag indeces for the output tags
	private List<Map<Key, List<Value>>> data;	
	private List<String> header;
	private Map<String, Tagger> taggers; // taggers for outTagKeys
	private Map<Integer, Tagger> otherTaggers; // taggers for userTags not included in the report

	public AllocationReport(AllocationConfig config, ResourceService resourceService) throws Exception {
    	super();
    	S3BucketConfig bucket = config.getS3Bucket();
    	withS3BucketConfig(new S3BucketConfig()
		.withName(bucket.getName())
		.withRegion(bucket.getRegion())
		.withPrefix(bucket.getPrefix())
		.withAccountId(bucket.getAccountId())
		.withAccessRole(bucket.getAccessRole())
		.withExternalId(bucket.getExternalId()));
    	
		this.config = config;
		this.header = Lists.newArrayList(new String[]{AllocationColumn.StartDate.toString(), AllocationColumn.EndDate.toString(), AllocationColumn.Allocation.toString()});
		this.inTagIndeces = Lists.newArrayList();
		this.outTagIndeces = Lists.newArrayList();
		this.data = Lists.newArrayList();
		
		this.inTagKeys = Lists.newArrayList(config.getIn().keySet());
		Collections.sort(this.inTagKeys);
		this.outTagKeys = Lists.newArrayList(config.getOut().keySet());
		Collections.sort(this.outTagKeys);
		
		for (String key: inTagKeys) {
			int index = resourceService.getUserTagIndex(key);
			if (index < 0 && !key.startsWith("_"))
				throw new Exception("Bad input tag index for key: \"" + key + "\"");
			inTagIndeces.add(index);
			String colName = config.getIn().get(key);
			if (header.contains(colName))
				throw new Exception("Duplicate input column name for key: \"" + key + "\", column name: \"" + colName + "\"");
			header.add(colName);
		}
		
		for (String key: outTagKeys) {
			outTagIndeces.add(resourceService.getUserTagIndex(key));
			String colName = config.getOut().get(key);
			if (header.contains(colName))
				throw new Exception("Duplicate output column name for key: \"" + key + "\", column name: \"" + colName + "\"");
			header.add(colName);
		}
		if (outTagIndeces.contains(-1))
			throw new Exception("Bad output tag index");
		
		
		taggers = Maps.newHashMap();
		otherTaggers = Maps.newHashMap();
		List<String> userTagKeys = resourceService.getCustomTags();

		if (config.getTagMaps() != null) {
			for (String tm: config.getTagMaps().keySet()) {
				if (outTagKeys.contains(tm)) {
					taggers.put(tm, new Tagger(config.getTagMaps().get(tm)));
				}
				else {
					otherTaggers.put(userTagKeys.indexOf(tm), new Tagger(config.getTagMaps().get(tm)));
				}
			}
		}
	}
	
	public List<String> getInTagKeys() {
		return inTagKeys;
	}
	
	public List<Integer> getInTagIndeces() {
		return inTagIndeces;
	}
	
	public List<String> getOutTagKeys() {
		return outTagKeys;
	}
	
	public class Tagger {
		private Map<String, Map<String, Comparator>> rules;
				
		Tagger(TagMappings tagMappings) {
			this.rules = Maps.newHashMap();
			
			Map<String, Map<String, List<String>>> maps = tagMappings.getMaps();
			rules = Maps.newHashMap();
			for (String dstValue: maps.keySet()) {
				Map<String, Comparator> srcKeyMap = Maps.newHashMap();
				for (String srcKey: maps.get(dstValue).keySet()) {
					srcKeyMap.put(srcKey, new Comparator(srcKey, maps.get(dstValue).get(srcKey)));
				}
				rules.put(dstValue, srcKeyMap);
			}
		}
		
		public class Comparator {
			private static final String regexPrefix = "re:";
			
			private int outputsIndex;
			private List<String> literals;
			private List<Pattern> patterns;
			
			Comparator(String key, List<String> patterns) {
				this.literals = Lists.newArrayList();
				this.patterns = Lists.newArrayList();
				
				// regular expressions have an "re:" prefix
				for (String p: patterns) {
					if (p.startsWith(regexPrefix)) {
						this.patterns.add(Pattern.compile(p.substring(regexPrefix.length())));
					}
					else {
						literals.add(p);
					}
				}
				this.outputsIndex = outTagKeys.indexOf(key);
			}
			
			boolean matches(List<String> values) {
				String value = values.get(outputsIndex);
				if (literals.contains(value))
					return true;
				for (Pattern p: patterns) {
					Matcher m = p.matcher(value);
					if (m.matches())
						return true;
				}
				return false;
			}
		}
		
		String get(Value value) {
			for (String dstValue: rules.keySet()) {
				for (String srcKey: rules.get(dstValue).keySet()) {
					if (rules.get(dstValue).get(srcKey).matches(value.getOutputs()))
						return dstValue;
				}
			}
			return "";
		}
	}
	
	public Key getKey(TagGroup tg) {
		List<String> inTags = Lists.newArrayListWithCapacity(32);
		for (String t: inTagKeys) {
			if (t.equals("_Account"))
				inTags.add(tg.account.getId());
			else if (t.equals("_Region"))
				inTags.add(tg.region.name);
			else if (t.equals("_Zone"))
				inTags.add(tg.zone.name);
			else if (t.equals("_Product"))
				inTags.add(tg.product.getServiceCode());
			else if (t.equals("_Operation"))
				inTags.add(tg.operation.name);
			else if (t.equals("_UsageType"))
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
		return data.size() > hour ? data.get(hour).get(key) : null;
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
		
		public String getOutput(String key) {
			return outputs.get(outTagKeys.indexOf(key));
		}
		
		public double getAllocation() {
			return allocation;
		}
		
		public TagGroup getOutputTagGroup(TagGroup tg) {
			UserTag[] userTags = tg.resourceGroup.getUserTags().clone();
			
			for (int i = 0; i < outputs.size(); i++) {
				String tag = outputs.get(i);
				if (tag.isEmpty()) {
					// Apply any mapping rules
					Tagger t = taggers.get(outTagKeys.get(i));
					tag = t == null ? "" : t.get(this);
					
					if (tag.isEmpty())
						continue;
				}
				userTags[outTagIndeces.get(i)] = UserTag.get(tag);
			}
			for (int i: otherTaggers.keySet()) {
				String tag = otherTaggers.get(i).get(this);
				if (tag.isEmpty())
					continue;
				userTags[i] = UserTag.get(tag);
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
	
	public void archiveReport(DateTime month, String filename, WorkBucketConfig workBucketConfig) throws Exception {
        File file = new File(workBucketConfig.localDir, filename);
        
    	OutputStream os = new FileOutputStream(file);
    	os = new GZIPOutputStream(os);
		Writer out = new OutputStreamWriter(os);
        
        try {
        	writeCsv(month, out);
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, workBucketConfig.localDir, file.getName());
        logger.info("uploaded " + file);
	}
    
	public boolean loadReport(DateTime month, String localDir) throws Exception {
		S3BucketConfig bucket = config.getS3Bucket();
        if (bucket.getName().isEmpty())
        	return false;
                    
        String prefix = bucket.getPrefix();

        String fileKey = prefix + AwsUtils.monthDateFormat.print(month);

        logger.info("trying to list objects in kubernetes report bucket " + bucket.getName() +
        		" using assume role \"" + bucket.getAccountId() + ":" + bucket.getAccessRole() + "\", and external id \"" + bucket.getExternalId() + "\" with key " + fileKey);
        
        List<S3ObjectSummary> objectSummaries = AwsUtils.listAllObjects(bucket.getName(), bucket.getRegion(), fileKey,
        		bucket.getAccountId(), bucket.getAccessRole(), bucket.getExternalId());
        logger.info("found " + objectSummaries.size() + " kubernetes report(s) in bucket " + bucket.getName());
        
        if (objectSummaries.size() == 0)
            return false;
        
        withS3ObjectSummary(objectSummaries.get(0));
        
		File file = download(localDir);
        logger.info("loading " + fileKey + "...");
		readFile(month, file);
        logger.info("done loading " + fileKey);
        return true;
	}

	private File download(String localDir) {
        String fileKey = getS3ObjectSummary().getKey();
		String prefix = fileKey.substring(0, fileKey.lastIndexOf("/") + 1);
		String filename = fileKey.substring(prefix.length());
        File file = new File(localDir, filename);

        if (getS3ObjectSummary().getLastModified().getTime() > file.lastModified()) {
	        logger.info("trying to download " + getS3ObjectSummary().getBucketName() + "/" + prefix + file.getName() + 
	        		" from account " + s3BucketConfig.getAccountId() + " using role " + s3BucketConfig.getAccessRole() + 
	        		(StringUtils.isEmpty(s3BucketConfig.getExternalId()) ? "" : " with exID: " + s3BucketConfig.getExternalId()) + "...");
	        boolean downloaded = AwsUtils.downloadFileIfChangedSince(getS3ObjectSummary().getBucketName(), s3BucketConfig.getRegion(), prefix, file, file.lastModified(),
	        		s3BucketConfig.getAccountId(), s3BucketConfig.getAccessRole(), s3BucketConfig.getExternalId());
	        if (downloaded)
	            logger.info("downloaded " + fileKey);
	        else {
	            logger.info("file already downloaded " + fileKey + "...");
	        }
        }

        return file;
	}
		
	protected void readFile(DateTime month, File file) {
		Reader reader = null;
        try {
            InputStream input = new FileInputStream(file);
            if (file.getName().endsWith(".gz")) {
            	input = new GZIPInputStream(input);
            }
            reader = new BufferedReader(new InputStreamReader(input));
            readCsv(month, reader);
        }
        catch (IOException e) {
            if (e.getMessage().equals("Stream closed"))
                logger.info("reached end of file.");
            else
                logger.error("Error processing " + file, e);
        }
        finally {
            if (reader != null)
                try {reader.close();} catch (Exception e) {}
        }
	}
    
    protected void readCsv(DateTime month, Reader reader) throws IOException {
    	data = Lists.newArrayList();
    	
    	Iterable<CSVRecord> records = CSVFormat.DEFAULT
    		      .withFirstRecordAsHeader()
    		      .parse(reader);
    	    	
    	header = null;
    	long monthMillis = month.getMillis();
    	
	    for (CSVRecord record : records) {
	    	if (header == null) {
	    		// get header
	    		header = record.getParser().getHeaderNames();
	    	}
	    	
	    	List<String> inTags = Lists.newArrayList();
	    	List<String> outTags = Lists.newArrayList();
	    	int startHour = (int) ((new DateTime(record.get(AllocationColumn.StartDate), DateTimeZone.UTC).getMillis() - monthMillis) / (1000 * 60 * 60));
	    	int endHour = (int) ((new DateTime(record.get(AllocationColumn.EndDate), DateTimeZone.UTC).getMillis() - monthMillis) / (1000 * 60 * 60));
	    	double allocation = Double.parseDouble(record.get(AllocationColumn.Allocation));
	    	if (Double.isNaN(allocation) || Double.isInfinite(allocation)) {
	    		logger.warn("Allocation report entry with NaN or Inf allocation, skipping.");
	    		continue;
	    	}
	    	for (String key: inTagKeys)
	    		inTags.add(record.get(config.getIn().get(key)));
	    	for (String key: outTagKeys)
	    		outTags.add(record.get(config.getOut().get(key)));
	    	for (int hour = startHour; hour < endHour; hour++)
	    		add(hour, allocation, inTags, outTags);
	    }
    }
    
    protected void writeCsv(DateTime month, Writer out) throws IOException {
    	String[] headerArray = new String[header.size()];
    	headerArray = header.toArray(headerArray);
        DateTimeFormatter isoFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC);
    	
    	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(headerArray));
    	
    	for (int hour = 0; hour < data.size(); hour++) {
    		Map<Key, List<Value>> hourAllocation = data.get(hour);
    		for (Key key: hourAllocation.keySet()) {
    			for (Value v: hourAllocation.get(key)) {
    				List<String> cols = Lists.newArrayList();
    				cols.add(month.plusHours(hour).toString(isoFormatter)); // StartDate
    				cols.add(month.plusHours(hour+1).toString(isoFormatter)); // EndDate
    				cols.add(Double.toString(v.allocation));
    				for (String inTag: key.getInputs())
    					cols.add(inTag);
    				for (String outTag: v.getOutputs())
    					cols.add(outTag);
    	    		printer.printRecord(cols);
    			}
    		}
    	}
    	printer.close(true);
    	
    }
}
