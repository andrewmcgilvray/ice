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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone.BadZone;

public class ReadWriteDataTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private static final String resourcesDir = "src/test/resources";
	private static final String dataDir = "src/test/data/";
    
    private static AccountService as;
    private static ProductService ps;
    
	private static Properties getProperties() throws IOException {
		Properties prop = new Properties();
		File file = new File(resourcesDir + "/ice.properties");
        InputStream is = new FileInputStream(file);
		prop.load(is);
	    is.close();
	    
		return prop;	
	}
	
	@BeforeClass
	public static void init() throws IOException {
		Properties p = getProperties();
		List<Account> accounts = Lists.newArrayList();
        for (String name: p.stringPropertyNames()) {
            if (name.startsWith("ice.account.")) {
                String accountName = name.substring("ice.account.".length());
                accounts.add(new Account(p.getProperty(name), accountName, null));
            }
        }
		as = new BasicAccountService(accounts);
        ps = new BasicProductService();
	}
	
	@Test
	public void testFileRead() throws IOException, BadZone {
        String filename = "cost_daily_505vubukj9ayygz7z5jbws97j_2020";
       
        File file = new File(dataDir, filename + ".gz");
        
        if (!file.exists()) {
        	// Don't run the test if the file doesn't exist
        	return;
        }
    	InputStream is = new FileInputStream(file);
    	is = new GZIPInputStream(is);
        DataInputStream in = new DataInputStream(is);
        ReadWriteData data = new ReadWriteData(0);
        
        try {
            data.deserialize(as, ps, in);
        }
        finally {
            if (in != null)
                in.close();
        }

        String outFilename = dataDir + "/" + filename + ".csv";
        
        FileWriter out;
		out = new FileWriter(outFilename);
        // Output CSV file
		ReadWriteDataTest.serialize(out, data);
    	out.close();
	}
	
	@Test
	public void testSerializeDeserializeRDS() throws IOException, BadZone {
		TagGroup tg = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.Rds), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		testSerializeDeserialize(tg, 1.0);
		
		tg = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.RdsFull), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		testSerializeDeserialize(tg, 1.0);
	}
	
	private void testSerializeDeserialize(TagGroup tg, Double value) throws IOException, BadZone {
		ReadWriteData data = new ReadWriteData(tg.resourceGroup == null ? 0 : tg.resourceGroup.getUserTags().length);
		data.enableTagGroupCache(true);
		
        List<Map<TagGroup, Double>> list = Lists.newArrayList();
        Map<TagGroup, Double> map = ReadWriteData.getCreateData(list, 0);
        map.put(tg, value);
		data.setData(list, 0);
		
		ReadWriteData result = serializeDeserialize(as, ps, data);
		result.enableTagGroupCache(true);
		
		assertEquals("Wrong number of tag groups in tagGroups", 1, result.getTagGroups().size());
		assertEquals("Length of data is wrong", 1, result.getNum());
		assertEquals("Length of first num is wrong", 1, result.getData(0).size());
		assertEquals("Value of first num is wrong", value, result.get(0, tg), 0.001);
	}
	
	@Test
	public void testSerializeDeserializeTwice() throws IOException, BadZone {
		ReadWriteData data = new ReadWriteData();
		data.enableTagGroupCache(true);
		
		TagGroup tg = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);
        List<Map<TagGroup, Double>> list = Lists.newArrayList();
        Map<TagGroup, Double> map = ReadWriteData.getCreateData(list, 0);
        map.put(tg, 1.0);
		data.setData(list, 0);
		
		data = serializeDeserialize(as, ps, data);
		
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);

		list = Lists.newArrayList();
		map = ReadWriteData.getCreateData(list, 0);
		map.put(tg2, 2.0);
		data.setData(list, 1);
		
		ReadWriteData result = serializeDeserialize(as, ps, data);
		result.enableTagGroupCache(true);
		
		assertEquals("Wrong number of tags in in tagGroups", 1, result.getTagGroups().size());
		assertEquals("Length of data is wrong", 2, result.getNum());
		assertEquals("Length of first num is wrong", 1, result.getData(0).size());
		assertEquals("Value of first num is wrong", 1.0, result.get(0, tg), 0.001);
		assertEquals("Length of second num is wrong", 1, result.getData(1).size());
		assertEquals("Value of second num is wrong", 2.0, result.get(1, tg2), 0.001);
		assertEquals("Tags don't match", tg, tg2);
	}
	
	ReadWriteData serializeDeserialize(AccountService as, ProductService ps, ReadWriteData data) throws IOException, BadZone {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(output);
		
		data.serialize(out, null);
		ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
		DataInput in = new DataInputStream(input);
		data = new ReadWriteData();
		data.enableTagGroupCache(true);
		data.deserialize(as, ps, in);
		return data;
	}
	
    public static void serialize(OutputStreamWriter out, ReadWriteData data) throws IOException {
    	out.write("num,data,account,region,zone,product,operation,usageType,usageUnits,resource\n");
        Collection<TagGroup> keys = data.getTagGroups();

        for (Integer i = 0; i < data.getNum(); i++) {
            Map<TagGroup, Double> map = data.getData(i);
            if (map.size() > 0) {
                for (TagGroup tagGroup: keys) {
                    Double v = map.get(tagGroup);
                    out.write(i.toString() + ",");
                    out.write(v == null ? "0," : (v.toString() + ","));
                    TagGroup.Serializer.serializeCsv(out, tagGroup);
                    out.write("\n");
                }
            }
        }
    }
    
    @Test
    public void testPutAll() {
    	// test the merging of two data sets.
    	ReadWriteData a = new ReadWriteData();
    	ReadWriteData b = new ReadWriteData();
    	
		TagGroup tg1 = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountByName("Account2"), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);
    	
    	a.put(0, tg1, 1.0);
    	a.put(0, tg2, 2.0);
    	b.put(0, tg2, 4.0);
    	a.putAll(b);
    	
    	assertEquals("TagGroup 1 is not correct", 1.0, a.get(0, tg1), .001);
    	assertEquals("TagGroup 2 is not correct", 6.0, a.get(0, tg2), .001);
    }
}
