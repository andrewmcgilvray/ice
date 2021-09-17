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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.junit.Test;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.processor.ReadWriteTagCoverageData;
import com.netflix.ice.processor.TagCoverageMetrics;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone.BadZone;

public class ReadOnlyTagCoverageDataTest {

	@Test
	public void testDeserialize() throws IOException, BadZone {
		AccountService as = new BasicAccountService();
		ProductService ps = new BasicProductService();
		
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(output);
		
        
        TagGroup tagGroup = TagGroup.getTagGroup(CostType.recurring, as.getAccountById("123", ""), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.ondemandInstances, UsageType.getUsageType("c1.medium", "hours"), null);
        int numTags = 5;
        TagCoverageMetrics metrics = new TagCoverageMetrics(numTags);
        TagCoverageMetrics.add(metrics, new boolean[]{ false, false, false, false, true });
        TagCoverageMetrics.add(metrics, new boolean[]{ false, false, false, true, true });
        TagCoverageMetrics.add(metrics, new boolean[]{ false, false, true, true, true });
        TagCoverageMetrics.add(metrics, new boolean[]{ false, true, true, true, true });

        ReadWriteTagCoverageData data = new ReadWriteTagCoverageData(numTags); 
        data.enableTagGroupCache(true);
        data.put(0, tagGroup, metrics);

        data.serialize(out, null);
        
		ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
		DataInput in = new DataInputStream(input);
		
		ReadOnlyTagCoverageData readOnlyData = new ReadOnlyTagCoverageData(numTags);
		readOnlyData.deserialize(as, ps, in);
		
		assertEquals("wrong data size", 1, readOnlyData.getNum());
		
		Collection<TagGroup> tgs = readOnlyData.getTagGroups(null, null, 0);
		assertEquals("wrong number of tag groups", 1, tgs.size());

		for (TagGroup tg: tgs) {
			TimeSeriesTagCoverageMetrics ts = readOnlyData.getData(tg);
			TagCoverageMetrics[] m = new TagCoverageMetrics[1];
			ts.get(0, 1, m);

			assertEquals("total is wrong", 4, m[0].getTotal());
			for (int i = 0; i < numTags; i++)
				assertEquals("count is wrong for index " + i, i, metrics.getCount(i));
		}

	}

}
