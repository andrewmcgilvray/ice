package com.netflix.ice.processor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.DataVersion;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Zone.BadZone;

public class DataSerializer implements ReadWriteDataSerializer, DataVersion {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	
    public static class CostAndUsage {
    	public final double cost;
    	public final double usage;
    	
    	public CostAndUsage(double cost, double usage) {
    		this.cost = cost;
    		this.usage = usage;
    	}
    	
    	public String toString() {
    		return "[" + Double.toString(cost) + "," + Double.toString(usage) + "]";
    	}
    }
    
    
	private List<Map<TagGroup, CostAndUsage>> data;
	
    // Cached set of tagGroup keys used throughout the list of data maps.
    // Post processing for reservations, savings plans, savings data, post processor, and data writing
    // all need an aggregated set of tagGroups and it's very expensive to walk the maps calling addAll().
    // This set is only maintained for the master CostAndUsageData container since it's only needed by
    // the post-processing steps. Individual CoatAndUsageData objects created for each separate CUR report
    // don't require it and run much faster if we don't have to maintain this master set.
    protected Set<TagGroup> tagGroups;
    
    // number of user tags in the resourceGroups. Set to -1 when constructed for deserialization.
    // will be initialized when read in.
    protected int numUserTags;
	
	public DataSerializer(ReadWriteData costData, ReadWriteData usageData) {
		this.numUserTags = costData.numUserTags;
		this.tagGroups = ConcurrentHashMap.newKeySet();
		this.tagGroups.addAll(costData.getTagGroups());
		this.tagGroups.addAll(usageData.getTagGroups());
		this.data = Lists.newArrayList();
		
		int max = Math.max(costData.getNum(), usageData.getNum());
		
        for (int i = 0; i < max; i++) {
            Map<TagGroup, Double> costMap = costData.getData(i);
            Map<TagGroup, Double> usageMap = usageData.getData(i);
            Map<TagGroup, CostAndUsage> cauMap = Maps.newHashMap();
            data.add(cauMap);
            
            if (costMap.isEmpty() && usageMap.isEmpty())
            	continue;
            
            for (TagGroup tagGroup: this.tagGroups) {
            	Double cost = costMap.get(tagGroup);
            	Double usage = usageMap.get(tagGroup);
            	if (cost == null && usage == null)
            		continue;
            	cauMap.put(tagGroup, new CostAndUsage(cost == null ? 0 : cost, usage == null ? 0 : usage));
            }
        }		
	}
	
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

    public int getNum() {
        return data.size();
    }

	public Map<TagGroup, CostAndUsage> getData(int i) {
        return Collections.unmodifiableMap(getCreateData(i));
	}

    public CostAndUsage get(int i, TagGroup tagGroup) {
    	return getData(i).get(tagGroup);
    }

    public void put(int i, TagGroup tagGroup, CostAndUsage value) {
    	getCreateData(i).put(tagGroup, value);
    	if (tagGroups != null)
    		tagGroups.add(tagGroup);
    }
    
    public void add(int i, TagGroup tagGroup, CostAndUsage value) {
    	Map<TagGroup, CostAndUsage> map = getCreateData(i);
    	CostAndUsage existing = map.get(tagGroup);
        map.put(tagGroup, existing == null ? value : new CostAndUsage(existing.cost + value.cost, existing.usage + value.usage));
    	if (tagGroups != null)
    		tagGroups.add(tagGroup);
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
                    existed.put(tg, existingValue == null ? value : new CostAndUsage(existingValue.cost + value.cost, existingValue.usage + value.usage));
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
     * 		6a. Data present for TagGroup flag (boolean)<br/>
     * 		6b. Data array for TagGroup consisting of a pair of cost and usage doubles (if flag is true)<br/>
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
        for (int i = 0; i < data.size(); i++) {
            Map<TagGroup, CostAndUsage> map = getData(i);
            out.writeBoolean(!map.isEmpty());
            if (!map.isEmpty()) {
                for (TagGroup tagGroup: keys) {
                	CostAndUsage v = map.get(tagGroup);
                    out.writeDouble(v == null ? 0 : v.cost);			
                    out.writeDouble(v == null ? 0 : v.usage);			
                }
            }
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
        for (int i = 0; i < num; i++)  {
            Map<TagGroup, CostAndUsage> map = Maps.newHashMap();
            boolean hasData = in.readBoolean();
            if (hasData) {
                for (int j = 0; j < keys.size(); j++) {
                    double cost = in.readDouble();
                    double usage = in.readDouble();
                    if (cost != 0 || usage != 0)
                    	map.put(keys.get(j), new CostAndUsage(cost, usage));
                }
            }
            data.add(map);
        }

        this.data = data;		
	}

}
