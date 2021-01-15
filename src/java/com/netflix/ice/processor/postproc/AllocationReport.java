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
import com.google.common.collect.Sets;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.Config.WorkBucketConfig;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.Report;
import com.netflix.ice.processor.TagMapper;
import com.netflix.ice.processor.config.S3BucketConfig;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UserTag;

/**
 * An allocation report contains the following columns:
 * 
 *  StartDate - Start hour for allocation. Minutes and seconds should always be zero. e.g. 2020-01-24T13:00:00Z
 *  EndDate - Non-inclusive end time for allocation. Minutes and seconds should always be zero. e.g. 2020-01-24T14:00:00Z
 *  Allocation - A decimal value typically between 0 and 1 indicating the portion of the input cost/usage to allocate to the specified output record
 *  
 *  One or more input tags used to filter the source data set and find the entries to apply the allocation in the row.
 *    Each input tag may be a literal value or an empty string. If empty, all source data with either an empty value or non-matching value in a different row for
 *    the dimension will produce a match.
 *  
 *  One or more output tags used to label the allocation.
 *
 */
public class AllocationReport extends Report {
    Logger logger = LoggerFactory.getLogger(getClass());
    
    enum AllocationColumn {
    	StartDate,
    	EndDate,
    	Allocation;
    }
	
	private AllocationConfig config;
	private long startMillis;
	private List<String> userTagKeys;
	private List<String> inTagKeys;
	private List<String> outTagKeys;
	private List<Integer> inTagIndeces; // ResourceGroup user tag indeces for the input tags
	private List<Integer> outTagIndeces; // ResourceGroup user tag indeces for the output tags
	private List<Set<String>> inTagValues; // Values used in the allocation report for each input tag key. Used to resolve empty strings for values in the report.
	private List<Map<Key, Map<Key, Double>>> data;	
	private List<String> header;
	private List<TagMapper> taggers;
	private List<String> newTagKeys;

	public AllocationReport(AllocationConfig config, long startMillis, boolean isReport, List<String> userTagKeys) throws Exception {
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
		this.startMillis = startMillis;
		this.userTagKeys = userTagKeys;
		this.header = Lists.newArrayList(new String[]{AllocationColumn.StartDate.toString(), AllocationColumn.EndDate.toString(), AllocationColumn.Allocation.toString()});
		this.inTagIndeces = Lists.newArrayList();
		this.outTagIndeces = Lists.newArrayList();
		this.newTagKeys = null;
		this.data = Lists.newArrayList();
		
		this.inTagKeys = Lists.newArrayList();
		if (config.getIn() != null)
			this.inTagKeys.addAll(config.getIn().keySet());
		this.inTagValues = Lists.newArrayList();
		for (int i = 0; i < inTagKeys.size(); i++)
			this.inTagValues.add(Sets.<String>newHashSet());
		
		Collections.sort(this.inTagKeys);
		this.outTagKeys = Lists.newArrayList(config.getOut().keySet());
		Collections.sort(this.outTagKeys);
		
		for (String key: inTagKeys) {
			int index = userTagKeys.indexOf(key);
			if (index < 0 && !key.startsWith("_"))
				throw new Exception("Bad input tag index for key: \"" + key + "\"");
			inTagIndeces.add(index);
			String colName = config.getIn().get(key);
			if (header.contains(colName))
				throw new Exception("Duplicate input column name for key: \"" + key + "\", column name: \"" + colName + "\"");
			header.add(colName);
		}
		
		for (String key: outTagKeys) {
			int index = userTagKeys.indexOf(key);
			if (index < 0) {
				if (isReport) {
					if (newTagKeys == null)
						newTagKeys = Lists.newArrayList();
					newTagKeys.add(key);
				}
				else {
					throw new Exception("Bad output tag index for key: " + key);
				}
			}
			outTagIndeces.add(index);
			String colName = config.getOut().get(key);
			if (header.contains(colName))
				throw new Exception("Duplicate output column name for key: \"" + key + "\", column name: \"" + colName + "\"");
			header.add(colName);
		}		
		
		taggers = Lists.newArrayList();

		if (config.getTagMaps() != null) {
			Map<String, Integer> tagKeyIndeces = Maps.newHashMap();
			int i = 0;
			for (String key: userTagKeys)
				tagKeyIndeces.put(key, i++);
			
			for (String tm: config.getTagMaps().keySet()) {
				taggers.add(new TagMapper(userTagKeys.indexOf(tm), config.getTagMaps().get(tm), tagKeyIndeces));
			}
		}
	}
	
	protected List<Map<Key, Map<Key, Double>>> getData() {
		return data;
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
	
	private Key getKey(int hour, TagGroup tg) throws Exception {
		List<String> inTags = Lists.newArrayListWithCapacity(32);
		for (int index = 0; index < inTagKeys.size(); index++) {
			String t = inTagKeys.get(index);
			String inTag = null;
			
			if (t.equals("_account"))
				inTag = tg.account.getId();
			else if (t.equals("_region"))
				inTag = tg.region.name;
			else if (t.equals("_zone"))
				inTag = tg.zone.name;
			else if (t.equals("_product"))
				inTag = tg.product.getServiceCode();
			else if (t.equals("_operation"))
				inTag = tg.operation.name;
			else if (t.equals("_usageType"))
				inTag = tg.usageType.name;
			else if (tg.resourceGroup != null) {
				UserTag[] userTags = tg.resourceGroup.getUserTags();
				for (int i = 0; i < inTagKeys.size(); i++) {
					if (t.equals(inTagKeys.get(i))) {
						if (inTagIndeces.get(i) < 0)
							throw new Exception("Unknown tag key name: " + t);
						inTag = userTags[inTagIndeces.get(i)].name;
						break;
					}
				}
			}
			
			// If not null, look up the tag value to see if we have it in the report
			if (inTag == null /* || !inTagValues.get(index).contains(inTag) */)
				inTag = "";
			
			inTags.add(inTag);
		}
		
		return findMostSpecificKey(hour, inTags);
	}
	
	/**
	 * Find the most specific key in the map that matches the set of input tags.
	 * Allocation report entries with empty strings for input tags will match
	 * a non-empty input value if no other entry with that value exists.
	 */
	private Key findMostSpecificKey(int hour, List<String> inTags) {		
		Map<Key, Map<Key, Double>> hourData = data.get(hour);
		
		// Check for an exact match first
		Key key = new Key(inTags);
		if (hourData.containsKey(key))
			return key;
		
		// No exact match, walk through the more general options
		List<Key> candidates = Lists.newArrayList();
		for (Key k: hourData.keySet()) {
			if (k.includes(key))
				candidates.add(k);
		}
		key = null;
		int maxValues = -1;
		for (Key k: candidates) {
			// TODO: May want to do this by precedence rather than number of values
			if (k.numValues() > maxValues) {
				maxValues = k.numValues();
				key = k;
			}
		}
		return key;
	}
	
	public Map<Key, Double> getData(int hour, TagGroup tg) throws Exception {
		if (data.size() <= hour)
			return null;
		
		Key key = getKey(hour, tg);

		return key == null ? null : data.get(hour).get(key);
	}
	
	public int getNumHours() {
		return data.size();
	}
	
	// For testing
	protected Map<Key, Double> getData(int hour, Key key) {
		return data.size() > hour ? data.get(hour).get(key) : null;
	}
	// For testing
	protected Set<Key> getKeySet(int hour) {
		return data.get(hour).keySet();
	}
	
	public void add(int hour, double allocation, List<String> inTags, List<String> outTags) {
		while (data.size() <= hour) {
			data.add(Maps.<Key, Map<Key, Double>>newHashMap());
		}
		Map<Key, Map<Key, Double>> hourData = data.get(hour);
		
		// Maintain sets of values for each input tag key
		for (int i = 0; i < inTags.size(); i++)
			inTagValues.get(i).add(inTags.get(i));
		
		Key k = new Key(inTags);
		Map<Key, Double> values = hourData.get(k);
		if (values == null) {
			values = Maps.newHashMap();
			hourData.put(k, values);
		}
		Key outKey = new Key(outTags);
		Double existing = values.get(outKey);
		Double value = allocation + (existing == null ? 0.0 : existing);
		values.put(outKey, value);
	}
	
	/**
	 * Return the set of allocation keys that exceed 100%
	 */
	public Map<Key, Double> overAllocatedKeys() {
		Map<Key, Double> keys = Maps.newHashMap();
		for (int hour = 0; hour < data.size(); hour++) {
			Map<Key, Map<Key, Double>> allocations = data.get(hour);
			for (Key key: allocations.keySet()) {
				Double total = 0.0;
				Map<Key, Double> outMap = allocations.get(key);
				for (Key outKey: outMap.keySet()) {
					total += outMap.get(outKey);
				}
				if (total > 1.000001) {
					Double existing = keys.get(key);
					if (existing == null || existing < total)
						keys.put(key, total);
				}
			}
		}
		return keys;
	}
			
	public static class Key {
		private List<String> tags;
		private int hashcode;
		private int numValues;
		
		protected Key(List<String> tags) {
			this.tags = tags;
			this.hashcode = genHashCode();
			this.numValues = 0;
			for (String tag: tags) {
				if (!tag.isEmpty())
					numValues++;
			}
		}
		
		@Override
		public String toString() {
			return tags.toString();
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
	        
	        for (int i = 0; i < tags.size(); i++) {
	        	if (!tags.get(i).equals(other.tags.get(i)))
	        		return false;
	        }
	        return true;
	    }
	    
	    private int genHashCode() {
	        final int prime = 31;
	        int result = 1;
	        for (String t: tags)
	        	result = prime * result + t.hashCode();

	        return result;
	    }
	    
	    public List<String> getTags() {
	    	return tags;
	    }
	    
	    /**
	     * Return true if the key is more general than the specified key.
	     * @return
	     */
	    public boolean includes(Key key) {
	    	for (int i = 0; i < tags.size(); i++) {
	    		String tag = tags.get(i);
	    		if (!tag.isEmpty() && !tag.equals(key.tags.get(i)))
	    			return false;
	    			
	    	}
	    	return true;
	    }
	    
	    public int numValues() {
	    	return numValues;
	    }
	}

	public String getTagValue(Key key, String tagKey) {
		return key.getTags().get(outTagKeys.indexOf(tagKey));
	}

	public TagGroup getOutputTagGroup(Key outputKey, TagGroup tg) {
		UserTag[] userTags = tg.resourceGroup.getUserTags();
		String[] tags = new String[userTagKeys.size()];
		for (int i = 0; i < userTags.length; i++)
			tags[i] = userTags[i].name;
		
		List<String> outputs = outputKey.getTags();
		
		// Assign values from allocation report
		for (int i = 0; i < outputs.size(); i++) {
			String tag = outputs.get(i);
			if (!tag.isEmpty()) // Only overwrite the existing value if we have something.
				tags[outTagIndeces.get(i)] = tag;
		}
		
		// Apply any mapping rules
		for (TagMapper tm: taggers) {
			int index = tm.getTagIndex();
			String tag = tm.apply(startMillis, tg.account.getId(), tags, tags[index]);
			tags[index] = tag;
		}
		try {
			return tg.withResourceGroup(ResourceGroup.getResourceGroup(tags));
		} catch (ResourceException e) {
			// should never throw because no user tags are null
			logger.error("error creating resource group from user tags: " + e);
		}
		return null;
	}

	public void archiveReport(DateTime month, String filename, WorkBucketConfig workBucketConfig) throws Exception {
        writeFile(month, workBucketConfig.localDir, filename, true);
        
        // archive to s3
        logger.info("uploading " + filename + "...");
        AwsUtils.upload(workBucketConfig.workS3BucketName, workBucketConfig.workS3BucketPrefix, workBucketConfig.localDir, filename);
        logger.info("uploaded " + filename);
	}
	
	protected void writeFile(DateTime month, String dir, String filename, boolean compress) throws Exception {
        File file = new File(dir, filename + (compress ? ".gz" : ""));
        
    	OutputStream os = new FileOutputStream(file);
    	if (compress)
    		os = new GZIPOutputStream(os);
		Writer out = new OutputStreamWriter(os);
        
        try {
        	writeCsv(month, out);
        }
        finally {
            out.close();
        }
	}
    
	public boolean loadReport(DateTime month, String localDir) throws Exception {
		S3BucketConfig bucket = config.getS3Bucket();
        if (bucket.getName().isEmpty())
        	return false;
                    
        String prefix = bucket.getPrefix();

        String fileKey = prefix + AwsUtils.monthDateFormat.print(month);

        logger.info("trying to list objects in report bucket " + bucket.getName() +
        		" using assume role \"" + bucket.getAccountId() + ":" + bucket.getAccessRole() + "\", and external id \"" + bucket.getExternalId() + "\" with key " + fileKey);
        
        List<S3ObjectSummary> objectSummaries = AwsUtils.listAllObjects(bucket.getName(), bucket.getRegion(), fileKey,
        		bucket.getAccountId(), bucket.getAccessRole(), bucket.getExternalId());
        logger.info("found " + objectSummaries.size() + " report(s) in bucket " + bucket.getName());
        
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
	    	try {
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
	    	catch (Exception e) {
	    		logger.error("Error processing record " + record.getRecordNumber() + ": \"" + record.toString() + "\" -- " + e.getMessage());
	    	}
	    }
    }
    
    protected void writeCsv(DateTime month, Writer out) throws IOException {
    	String[] headerArray = new String[header.size()];
    	headerArray = header.toArray(headerArray);
        DateTimeFormatter isoFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC);
    	
    	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(headerArray));
    	
    	for (int hour = 0; hour < data.size(); hour++) {
    		Map<Key, Map<Key, Double>> hourAllocation = data.get(hour);
    		for (Key key: hourAllocation.keySet()) {
    			Map<Key, Double> outMap = hourAllocation.get(key);
    			for (Key outKey: outMap.keySet()) {
    				List<String> cols = Lists.newArrayList();
    				cols.add(month.plusHours(hour).toString(isoFormatter)); // StartDate
    				cols.add(month.plusHours(hour+1).toString(isoFormatter)); // EndDate
    				cols.add(Double.toString(outMap.get(outKey)));
    				for (String inTag: key.getTags())
    					cols.add(inTag);
    				for (String outTag: outKey.getTags())
    					cols.add(outTag);
    	    		printer.printRecord(cols);
    			}
    		}
    	}
    	printer.close(true);
    	
    }
}
