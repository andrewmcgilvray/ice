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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.netflix.ice.tag.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.DataVersion;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;

public abstract class ReadOnlyGenericData<D> implements DataVersion {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	protected Map<TagGroup, D> data;
	protected int numIntervals;
	protected int numUserTags;
	private Map<TagType, Map<Tag, List<TagGroup>>> tagGroupsByTagAndTagType;
	private List<Map<Tag, List<TagGroup>>> tagGroupsByUserTag;

    final static TagType[] tagTypes = new TagType[]{ TagType.CostType, TagType.Account, TagType.Region, TagType.Zone, TagType.Product, TagType.Operation, TagType.UsageType };

	public ReadOnlyGenericData(Map<TagGroup, D> data, int numUserTags, int numIntervals) {
		this.data = data;
		this.numUserTags = numUserTags;
		this.numIntervals = numIntervals;
		buildIndecies();
	}

	public int numTagGroups() {
		return data.size();
	}

    public int getNum() {
        return numIntervals;
    }

	public D getData(TagGroup tg) {
		return data.get(tg);
	}

	public Collection<TagGroup> getTagGroups(TagType groupBy, Tag tag, int userTagIndex) {
		if (groupBy == null)
			return data.keySet();

		Map<Tag, List<TagGroup>> byTag = groupBy == TagType.Tag ? tagGroupsByUserTag.get(userTagIndex) : tagGroupsByTagAndTagType.get(groupBy);
		List<TagGroup> tagGroups = byTag == null ? null : byTag.get(tag);
		return tagGroups;
	}

	abstract protected void deserializeTimeSeriesData(List<TagGroup> keys, DataInput in) throws IOException;

	public void deserialize(AccountService accountService, ProductService productService, DataInput in) throws IOException, Zone.BadZone {
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
			if (tg.resourceGroup != null && tg.resourceGroup.getUserTags().length != numUserTags)
				logger.error("Wrong number of user tags: " + tg);
			keys.add(tg);
		}

		this.numUserTags = numUserTags;
		this.numIntervals = in.readInt();
		deserializeTimeSeriesData(keys, in);
		buildIndecies();
	}

	protected void buildIndecies() {
		// Build the account-based TagGroup maps
		tagGroupsByTagAndTagType = Maps.newHashMap();
		for (TagType t: tagTypes)
			tagGroupsByTagAndTagType.put(t, Maps.<Tag, List<TagGroup>>newHashMap());

		if (numUserTags > 0) {
			tagGroupsByUserTag = Lists.newArrayList();
			for (int i = 0; i < numUserTags; i++)
				tagGroupsByUserTag.add(Maps.<Tag, List<TagGroup>>newHashMap());
		}

		for (TagGroup tg: data.keySet()) {
			addIndex(tagGroupsByTagAndTagType.get(TagType.CostType), tg.costType, tg);
			addIndex(tagGroupsByTagAndTagType.get(TagType.Account), tg.account, tg);
			addIndex(tagGroupsByTagAndTagType.get(TagType.Region), tg.region, tg);
			addIndex(tagGroupsByTagAndTagType.get(TagType.Zone), tg.zone, tg);
			addIndex(tagGroupsByTagAndTagType.get(TagType.Product), tg.product, tg);
			addIndex(tagGroupsByTagAndTagType.get(TagType.Operation), tg.operation, tg);
			addIndex(tagGroupsByTagAndTagType.get(TagType.UsageType), tg.usageType, tg);

			if (numUserTags > 0) {
				if (tg.resourceGroup == null) {
					for (int j = 0; j < numUserTags; j++)
						addIndex(tagGroupsByUserTag.get(j), UserTag.empty, tg);
				}
				else {
					UserTag[] userTags = tg.resourceGroup.getUserTags();
					for (int j = 0; j < numUserTags; j++)
						addIndex(tagGroupsByUserTag.get(j), userTags[j], tg);
				}
			}
		}
	}

	private void addIndex(Map<Tag, List<TagGroup>> indecies, Tag tag, TagGroup tagGroup) {
		List<TagGroup> l = indecies.get(tag);
		if (l == null) {
			indecies.put(tag, Lists.<TagGroup>newArrayList());
			l = indecies.get(tag);
		}
		l.add(tagGroup);
	}
}
