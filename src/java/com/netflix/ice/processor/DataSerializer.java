package com.netflix.ice.processor;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.ice.common.TimeSeriesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.DataVersion;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Zone.BadZone;

public class DataSerializer implements ReadWriteDataSerializer, DataVersion {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static class CostAndUsage {
    	public final double cost;
    	public final double usage;
    	
    	public CostAndUsage() {
    		this.cost = 0;
    		this.usage = 0;
    	}
    	
    	public CostAndUsage(double cost, double usage) {
    		this.cost = cost;
    		this.usage = usage;
    	}
    	
    	public String toString() {
    		return "{cost: " + Double.toString(cost) + ", usage: " + Double.toString(usage) + "}";
    	}
    	
    	public boolean isZero() {
    		return cost == 0.0 && usage == 0.0;
    	}

    	public CostAndUsage add(CostAndUsage value) {
    		if (value == null || value.isZero())
    			return this;
    		
    		return new CostAndUsage(cost + value.cost, usage + value.usage);
    	}
    	
    	public CostAndUsage add(double otherCost, double otherUsage) {
    		if (otherCost == 0 && otherUsage == 0)
    			return this;
    		
    		return new CostAndUsage(cost + otherCost, usage + otherUsage);
    	}
    	
    	public CostAndUsage sub(CostAndUsage value) {
    		if (value == null || value.isZero())
    			return this;
    		
    		return new CostAndUsage(cost - value.cost, usage - value.usage);
    	}

    	public boolean equals(CostAndUsage o) {
    		return cost == o.cost && usage == o.usage;
		}
    	
    }
    
    
	protected List<Map<TagGroup, CostAndUsage>> data;
	
    // Cached set of tagGroup keys used throughout the list of data maps.
    // Post processing for reservations, savings plans, savings data, post processor, and data writing
    // all need an aggregated set of tagGroups and it's very expensive to walk the maps calling addAll().
    // This set is only maintained for the master CostAndUsageData container since it's only needed by
    // the post-processing steps. Individual CoatAndUsageData objects created for each separate CUR report
    // don't require it and run much faster if we don't have to maintain this master set.
    protected Collection<TagGroup> tagGroups;
    
    // number of user tags in the resourceGroups. Set to -1 when constructed for deserialization.
    // will be initialized when read in.
    protected int numUserTags;
		
	public DataSerializer(int numUserTags) {
		this.numUserTags = numUserTags;
		this.tagGroups = ConcurrentHashMap.newKeySet();
		this.data = Lists.newArrayList();
	}

	static public Map<TagGroup, CostAndUsage> getCreateData(List<Map<TagGroup, CostAndUsage>> data, int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, CostAndUsage>newHashMap());
            }
        }
        return data.get(i);
    }

	public void enableTagGroupCache(boolean enabled) {
		if (!enabled)
			tagGroups = null;
		else if (tagGroups == null) {
			tagGroups = ConcurrentHashMap.newKeySet();
	        for (int i = 0; i < data.size(); i++) {
        		tagGroups.addAll(data.get(i).keySet());	        	
	        }
		}
	}
	
	public String toString() {
		final int max = 2560;
		StringBuffer sb = new StringBuffer();
		sb.append("[\n");
		for (Map<TagGroup, CostAndUsage> map: data) {
			sb.append("  {\n");
			for (TagGroup tg: map.keySet()) {
				sb.append("    " + tg.toString() + ": " + map.get(tg).toString() + "\n");
				if (sb.length() > max) {
					sb.append("...");
					break;
				}
			}
			sb.append("  },\n");
			if (sb.length() > max) {
				sb.append("...");
				break;
			}
		}
		sb.append("]\n");
		return sb.toString();
	}


    /**
     * Gets the aggregated set of TagGroups across all time intervals in the list of maps.
     *
     * @return a set of TagGroups
     */
    public Collection<TagGroup> getTagGroups() {
        return tagGroups;
    }

    /**
     * Gets the tagGroup key set for the given hour
     */
    public Collection<TagGroup> getTagGroups(int i) {
    	return getData(i).keySet();
    }

    public int getNum() {
        return data.size();
    }

	public Map<TagGroup, CostAndUsage> getData(int i) {
        return Collections.unmodifiableMap(getCreateData(i));
	}

    public CostAndUsage get(int i, TagGroup tagGroup) {
    	return getData(i).get(tagGroup);
    }

    void cutData(int num) {
        if (data.size() > num)
            data = data.subList(0, num);
    }

    public void put(int i, TagGroup tagGroup, CostAndUsage value) {
    	getCreateData(i).put(tagGroup, value);
    	if (tagGroups != null)
    		tagGroups.add(tagGroup);
    }
    
    public void add(int i, TagGroup tagGroup, CostAndUsage value) {
    	Map<TagGroup, CostAndUsage> map = getCreateData(i);
        map.put(tagGroup, value.add(map.get(tagGroup)));
    	if (tagGroups != null)
    		tagGroups.add(tagGroup);
    }

    public void add(int i, TagGroup tagGroup, double cost, double usage) {
    	Map<TagGroup, CostAndUsage> map = getCreateData(i);
    	CostAndUsage existing = map.get(tagGroup);
        map.put(tagGroup, existing != null ? existing.add(cost, usage) : new CostAndUsage(cost, usage));
    	if (tagGroups != null)
    		tagGroups.add(tagGroup);
    }

    public CostAndUsage remove(int i, TagGroup tagGroup) {
    	if (i >= data.size())
    		return null;
    	CostAndUsage existing = data.get(i).remove(tagGroup);
    	if (existing != null) {
    		// See if we can purge the value from the cache
    		boolean found = false;
    		// Most remove calls are done by RI and SP processors which
    		// run through the data lists from 0 to the end.
    		// Assuming most values are present throughout the list, we
    		// can save time by checking from back to front since we'll
    		// bail quickly if we find the tagGroup at the end.
    		for (int j = data.size() - 1; j >= 0; j--) {
    			Map<TagGroup, CostAndUsage> d = data.get(j);
    			if (d.containsKey(tagGroup)) {
    				found = true;
    				break;
    			}
    		}
    		if (!found && tagGroups != null)
    			tagGroups.remove(tagGroup);
    	}

    	return existing;
    }

    /**
     * Set the supplied data in the map. Called by the cost and usage data archiver to merge summary data.
     */
    public void setData(List<Map<TagGroup, CostAndUsage>> newData, int startIndex) {
        for (int i = 0; i < newData.size(); i++) {
            int index = startIndex + i;

            if (index > data.size()) {
                getCreateData(index-1);
            }
            if (index >= data.size()) {
                data.add(newData.get(i));
            }
            else {
            	Map<TagGroup, CostAndUsage> removed = data.set(index, newData.get(i));
            	if (removed != null) {
	            	for (TagGroup tg: removed.keySet()) {
	            		// See if we can purge the value from the cache
	            		boolean found = false;
	            		for (int j = 0; j < data.size(); j++) {
	            			Map<TagGroup, CostAndUsage> d = data.get(j);
	            			if (d.containsKey(tg)) {
	            				found = true;
	            				break;
	            			}
	            		}
	            		if (!found && tagGroups != null)
	            			tagGroups.remove(tg);
	            	}
            	}
            }
        	if (tagGroups != null)
        		tagGroups.addAll(newData.get(i).keySet());
        }
    }

    /**
     * Merge all the data from the source into the existing destination.
     */
    void putAll(DataSerializer srcData) {
    	List<Map<TagGroup, CostAndUsage>> newData = srcData.data;
        for (int i = 0; i < newData.size(); i++) {
            if (i > data.size()) {
                getCreateData(i-1);
            }
            if (i >= data.size()) {
                data.add(newData.get(i));
            }
            else {
                Map<TagGroup, CostAndUsage> existed = data.get(i);
                for (TagGroup tg: newData.get(i).keySet()) {
                	CostAndUsage existingValue = existed.get(tg);
                	CostAndUsage value = newData.get(i).get(tg);
                    existed.put(tg, existingValue == null ? value : value.add(existingValue));
                }
            }
        }
        // Update the tagGroups cache as appropriate
    	if (tagGroups != null) {
    		if (srcData.tagGroups != null) {
    			// pull from src cache
    			tagGroups.addAll(srcData.tagGroups);
    		}
    		else {
    			// get it from each interval
    			for (int i = 0; i < newData.size(); i++)
    				tagGroups.addAll(newData.get(i).keySet());
    		}
    	}
    }

    Map<TagGroup, CostAndUsage> getCreateData(int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, CostAndUsage>newHashMap());
            }
        }
        return data.get(i);
    }

    /**
     * Serialize data using standard Java serialization DataOutput methods in the following order:<br/>
     *
     * 1. Version (int)<br/>
     * 2. Number of user tags (int)<br/>
     * 3. TagGroup count (int)<br/>
     * 4. TagGroup Array<br/>
     * 5. Number of hours/days/weeks/months of data (int)<br/>
     * 6. Data matrix:<br/>
     * 		6a. Data present for time interval flag (boolean)<br/>
     * 		6b. Data array for time interval consisting of a pair of cost and usage doubles for each TagGroup (if flag is true)<br/>
     */
	@Override
	public void serialize(DataOutput out, TagGroupFilter filter)
			throws IOException {
        Collection<TagGroup> keys = tagGroups;

    	if (numUserTags == -1 && keys.size() > 0) {
     		logger.warn("Error attempting to serialize data without setting the number of user tags. Pulling value from one of the tag groups");
       		TagGroup first = keys.iterator().next();
       		numUserTags = first.resourceGroup == null ? 0 : first.resourceGroup.getUserTags().length;
    	}

        if (filter != null)
        	keys = filter.getTagGroups(keys);

        out.writeInt(CUR_WORK_BUCKET_VERSION);
        out.writeInt(numUserTags);
        out.writeInt(keys.size());
        for (TagGroup tagGroup: keys) {
            TagGroup.Serializer.serialize(out, tagGroup);
        }

		out.writeInt(data.size());

		double cost[] = new double[data.size()];
		double usage[] = new double[data.size()];

		for (TagGroup tagGroup: keys) {
			for (int i = 0; i < data.size(); i++) {
				Map<TagGroup, CostAndUsage> map = getData(i);
				if (map.isEmpty()) {
					cost[i] = 0;
					usage[i] = 0;
				}
				else {
					CostAndUsage v = map.get(tagGroup);
					cost[i] = v == null ? 0 : v.cost;
					usage[i] = v == null ? 0 : v.usage;
				}
			}
			TimeSeriesData tsd = new TimeSeriesData(cost, usage);
			tsd.serialize(out);
		}
	}

	@Override
	public void deserialize(AccountService accountService,
			ProductService productService, DataInput in) throws IOException,
			BadZone {
    	int version = in.readInt();
    	// Verify that the file version matches
    	if (version != CUR_WORK_BUCKET_VERSION) {
    		throw new IOException("Wrong file version, expected " + CUR_WORK_BUCKET_VERSION + ", got " + version);
    	}
        numUserTags = in.readInt();
        int numKeys = in.readInt();
        List<TagGroup> keys = Lists.newArrayList();
        for (int j = 0; j < numKeys; j++) {
        	TagGroup tg = TagGroup.Serializer.deserialize(accountService, productService, numUserTags, in);
            keys.add(tg);
        	if (tagGroups != null)
        		tagGroups.add(tg);
        }

        List<Map<TagGroup, CostAndUsage>> data = Lists.newArrayList();
        int num = in.readInt();
        for (int i = 0; i < num; i++)
        	data.add(Maps.<TagGroup, CostAndUsage>newHashMap());

		boolean timeSeries = true;
		double cost[] = new double[num];
		double usage[] = new double[num];
		for (TagGroup tagGroup: keys) {
			TimeSeriesData tsd = TimeSeriesData.deserialize(in);
			tsd.get(TimeSeriesData.Type.COST, 0, num, cost);
			tsd.get(TimeSeriesData.Type.USAGE, 0, num, usage);
			for (int i = 0; i < num; i++) {
				if (cost[i] != 0 || usage[i] != 0) {
					CostAndUsage cau = new CostAndUsage(cost[i], usage[i]);
					data.get(i).put(tagGroup, cau);
				}
			}
		}

        this.data = data;		
	}

    public void serializeCsv(OutputStreamWriter out, String resourceGroupHeader) throws IOException {
    	// write the header
    	out.write("index,");
    	TagGroup.Serializer.serializeCsvHeader(out, resourceGroupHeader);
    	out.write(",cost,usage\n");
        for (int i = 0; i < data.size(); i++) {
            Map<TagGroup, CostAndUsage> map = getData(i);
            for (Entry<TagGroup, CostAndUsage> entry: map.entrySet()) {
            	out.write("" + i + ",");
            	TagGroup.Serializer.serializeCsv(out, entry.getKey());
                out.write(",");
                CostAndUsage v = entry.getValue();
                if (v == null)
                	out.write("0.0,0.0");
                else
                	out.write(Double.toString(v.cost) + "," + Double.toString(v.usage));
                out.write("\n");
            }
        }
    }
    
    public void deserializeCsv(AccountService accountService, ProductService productService, BufferedReader in) throws IOException, BadZone {
    	final int resourceStartIndex = 9; // 9 dimensions (index, costType, account, region, zone, product, operation, usageType, units)  plus cost and usage columns
    	final int numNonResourceColumns = resourceStartIndex + 2; // plus cost and usage columns
        List<Map<TagGroup, CostAndUsage>> data = Lists.newArrayList();
        
        String line;
        
        // skip the header
        in.readLine();

        Map<TagGroup, CostAndUsage> map = null;
        while ((line = in.readLine()) != null) {
        	String[] items = line.split(",");
        	int hour = Integer.parseInt(items[0]);
        	while (hour >= data.size()) {
        		map = Maps.newHashMap();
        		data.add(map);
        	}
        	map = data.get(hour);
        	String[] resourceGroup = null;
        	if (items.length > numNonResourceColumns) {
	        	resourceGroup = new String[items.length - numNonResourceColumns];
	        	for (int i = 0; i < items.length - numNonResourceColumns; i++)
	        		resourceGroup[i] = items[i + resourceStartIndex];
        	}
        	TagGroup tag = null;
			try {
				tag = TagGroup.getTagGroup(items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], resourceGroup, accountService, productService);
			} catch (ResourceException e) {
				// Should never throw because no user tags are null
			}
        	Double cost = Double.parseDouble(items[items.length-2]);
        	Double usage = Double.parseDouble(items[items.length-1]);
        	map.put(tag, new CostAndUsage(cost, usage));
        }

        this.data = data;
    }
}
