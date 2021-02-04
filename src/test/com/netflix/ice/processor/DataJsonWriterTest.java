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
package com.netflix.ice.processor;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Product.Code;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.UserTagKey;
import com.netflix.ice.tag.Zone.BadZone;

public class DataJsonWriterTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());

	private ProductService productService = new BasicProductService();

	private TagGroup getTagGroup(Code product, String operation, String usageType, String[] resource) throws BadZone, ResourceException {
		Product prod = productService.getProduct(product);
		return TagGroup.getTagGroup(
				new Account("123456789012", "a1", Lists.<String>newArrayList()),
				Region.US_EAST_1, Region.US_EAST_1.getZone("us-east-1a"), 
				prod, 
				Operation.getOperation(operation), 
				UsageType.getUsageType(usageType, ""), 
				resource == null ? null : ResourceGroup.getResourceGroup(resource));
	}
	
	class Data {
		class TestDataSerializer extends DataSerializer {
			public TestDataSerializer(int numUserTags) {
				super(numUserTags);
			}

			void setData(TagGroup tg, Double cost, Double usage, int i) {
	            if (i >= data.size()) {
	                getCreateData(i);
	            }
	            data.get(i).put(tg, new CostAndUsage(cost, usage));
			}
		}
	    public Map<Product, DataSerializer> dataByProduct;
	    public int numUserTags;
		
	    public Data(int numUserTags) {
	    	this.dataByProduct = Maps.newHashMap();
	    	this.numUserTags = numUserTags;
	    }
	    	    
	    public void add(TagGroup tg, Double cost, Double usage) {
    		if (!dataByProduct.containsKey(tg.product))
    			dataByProduct.put(tg.product, new TestDataSerializer(numUserTags));
    			
    		((TestDataSerializer)dataByProduct.get(tg.product)).setData(tg, cost, usage, 0);
	    }
	}
	
	@Test
	public void testWrite() throws Exception {
		Data data = new Data(2);
		
		data.add(getTagGroup(Code.S3, "CopyObject", "Requests-Tier1", null), 1.11, 1.0);
		data.add(getTagGroup(Code.S3, "GetObject", "Requests-Tier2", new String[]{"foo", "bar"}), 2.22, 2.0);
		
		DateTime dt = DateTime.parse("2017-08-01T00:00:00Z");
		
		List<UserTagKey> tagKeys = Lists.newArrayList();
		tagKeys.add(UserTagKey.get("Tag1"));
		tagKeys.add(UserTagKey.get("Tag2"));
		
		DataJsonWriter djw = new DataJsonWriter(dt, tagKeys, data.dataByProduct);
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		djw.writer = new OutputStreamWriter(out);		

		djw.write(null);
		djw.writer.flush();
		
		String expectGet =
				"{" +
					"\"hour\":\"2017-08-01T00:00:00Z\","+
					"\"org\":\"\","+
					"\"costType\":\"Recurring\","+
					"\"accountId\":\"123456789012\","+
					"\"account\":\"a1\","+
					"\"region\":\"us-east-1\","+
					"\"zone\":\"us-east-1a\","+
					"\"product\":\"S3\","+
					"\"operation\":\"GetObject\","+
					"\"usageType\":\"Requests-Tier2\","+
					"\"tags\":{\"Tag1\":\"foo\",\"Tag2\":\"bar\"},"+
					"\"cost\":2.22,"+
					"\"usage\":2.0"+
				"}";
		String expectCopy =
				"{" +
					"\"hour\":\"2017-08-01T00:00:00Z\","+
					"\"org\":\"\","+
					"\"costType\":\"Recurring\","+
					"\"accountId\":\"123456789012\","+
					"\"account\":\"a1\","+
					"\"region\":\"us-east-1\","+
					"\"zone\":\"us-east-1a\","+
					"\"product\":\"S3\","+
					"\"operation\":\"CopyObject\","+
					"\"usageType\":\"Requests-Tier1\","+
					"\"cost\":1.11,"+
					"\"usage\":1.0"+
				"}";
		
		String outString = new String(out.toByteArray());
		//logger.info(outString);
		String[] got = outString.split("\n");
		boolean foundCopy = false;
		boolean foundGet = false;
		for (int i = 0; i < 2; i++) {
			if (got[i].contains("Get")) {
				assertEquals("Incorrect JSON serialization", expectGet, got[i]);
				foundGet = true;
			}
			else if (got[i].contains("Copy")) {
				assertEquals("Incorrect JSON serialization", expectCopy, got[i]);
				foundCopy = true;
			}
		}
		assertTrue("Did not find both records", foundCopy && foundGet);
	}

}
