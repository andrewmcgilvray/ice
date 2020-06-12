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

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.DataVersion;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.Zone.BadZone;

public abstract class ReadOnlyGenericData<D> implements DataVersion {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected D[][] data;
    protected List<TagGroup> tagGroups;
    private Map<TagType, Map<Tag, Map<TagGroup, Integer>>> tagGroupsByTagAndTagType;
    protected int numUserTags;
    private List<Map<Tag, Map<TagGroup, Integer>>> tagGroupsByUserTag;

    final static TagType[] tagTypes = new TagType[]{ TagType.Account, TagType.Region, TagType.Zone, TagType.Product, TagType.Operation, TagType.UsageType };

    public ReadOnlyGenericData(D[][] data, List<TagGroup> tagGroups, int numUserTags) {
        this.data = data;
        this.tagGroups = tagGroups;
        this.numUserTags = numUserTags;
        buildIndecies();
    }

    public D[] getData(int i) {
        return data[i];
    }

    public int getNum() {
        return data.length;
    }

    public List<TagGroup> getTagGroups() {
        return tagGroups;
    }

    public Map<TagGroup, Integer> getTagGroups(TagType groupBy, Tag tag, int userTagIndex) {
    	Map<Tag, Map<TagGroup, Integer>> byTag = groupBy == TagType.Tag ? tagGroupsByUserTag.get(userTagIndex) : tagGroupsByTagAndTagType.get(groupBy);
        return byTag == null ? null : byTag.get(tag);
    }

    abstract protected D[][] newDataMatrix(int size);
    abstract protected D[] newDataArray(int size);
    abstract protected D readValue(DataInput in) throws IOException ;

    public void deserialize(AccountService accountService, ProductService productService, DataInput in) throws IOException, BadZone {
    	deserialize(accountService, productService, in, true);
    }

    public void deserialize(AccountService accountService, ProductService productService, DataInput in, boolean buildIndecies) throws IOException, BadZone {
    	int version = in.readInt();
    	// Verify that the file version matches
    	if (version != CUR_WORK_BUCKET_VERSION) {
    		throw new IOException("Wrong file version, expected " + CUR_WORK_BUCKET_VERSION + ", got " + version);
    	}
    	int numUserTags = in.readInt();
    	if (numUserTags != this.numUserTags)
    		logger.error("Data file has wrong number of user tags, expected " + this.numUserTags + ", got " + numUserTags);

        int numKeys = in.readInt();
        List<TagGroup> keys = Lists.newArrayList();
        for (int j = 0; j < numKeys; j++) {
        	TagGroup tg = TagGroup.Serializer.deserialize(accountService, productService, numUserTags, in);
//        	if (keys.contains(tg))
//        		logger.error("Duplicate tag group in data file: " + tg + ", existing at index: " + keys.indexOf(tg));
        	if (tg.resourceGroup != null && tg.resourceGroup.getUserTags().length != numUserTags)
        		logger.error("Wrong number of user tags: " + tg);
            keys.add(tg);
        }

        int num = in.readInt();
        D[][] data = newDataMatrix(num);
        for (int i = 0; i < num; i++)  {
        	data[i] = null;
            boolean hasData = in.readBoolean();
            if (hasData) {
                data[i] = newDataArray(keys.size());
                for (int j = 0; j < keys.size(); j++) {
                    D v = readValue(in);
                    if (v != null) {
                        data[i][j] = v;
                    }
                }
            }
        }

        this.data = data;
        this.tagGroups = keys;
        this.numUserTags = numUserTags;
        if (buildIndecies)
        	buildIndecies();
    }

    protected void buildIndecies() {
    	// Build the account-based TagGroup maps
    	tagGroupsByTagAndTagType = Maps.newHashMap();
    	for (TagType t: tagTypes)
    		tagGroupsByTagAndTagType.put(t, Maps.<Tag, Map<TagGroup, Integer>>newHashMap());

    	if (numUserTags > 0) {
	    	tagGroupsByUserTag = Lists.newArrayList();
	    	for (int i = 0; i < numUserTags; i++)
	    		tagGroupsByUserTag.add(Maps.<Tag, Map<TagGroup, Integer>>newHashMap());
    	}

    	for (int i = 0; i < tagGroups.size(); i++) {
    		TagGroup tg = tagGroups.get(i);
    		addIndex(tagGroupsByTagAndTagType.get(TagType.Account), tg.account, tg, i);
    		addIndex(tagGroupsByTagAndTagType.get(TagType.Region), tg.region, tg, i);
    		addIndex(tagGroupsByTagAndTagType.get(TagType.Zone), tg.zone, tg, i);
    		addIndex(tagGroupsByTagAndTagType.get(TagType.Product), tg.product, tg, i);
    		addIndex(tagGroupsByTagAndTagType.get(TagType.Operation), tg.operation, tg, i);
    		addIndex(tagGroupsByTagAndTagType.get(TagType.UsageType), tg.usageType, tg, i);

    		if (numUserTags > 0) {
	    		if (tg.resourceGroup == null) {
		    		for (int j = 0; j < numUserTags; j++)
		    			addIndex(tagGroupsByUserTag.get(j), UserTag.empty, tg, i);
	    		}
				else {
		    		UserTag[] userTags = tg.resourceGroup.getUserTags();
		    		for (int j = 0; j < numUserTags; j++)
		    			addIndex(tagGroupsByUserTag.get(j), userTags[j], tg, i);
				}
    		}
    	}
    }

    private void addIndex(Map<Tag, Map<TagGroup, Integer>> indecies, Tag tag, TagGroup tagGroup, int index) {
		Map<TagGroup, Integer> m = indecies.get(tag);
		if (m == null) {
			indecies.put(tag,  Maps.<TagGroup, Integer>newHashMap());
			m = indecies.get(tag);
		}
		m.put(tagGroup, index);
    }
}
