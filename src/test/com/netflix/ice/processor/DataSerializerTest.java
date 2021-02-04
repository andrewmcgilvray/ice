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
import com.netflix.ice.processor.DataSerializer.CostAndUsage;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone.BadZone;

public class DataSerializerTest {
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
        DataSerializer data = new DataSerializer(0);
        
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
		DataSerializerTest.serialize(out, data);
    	out.close();
	}
	
	@Test
	public void testSerializeDeserializeRDS() throws IOException, BadZone {
		TagGroup tg = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.Rds), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		testSerializeDeserialize(tg, new CostAndUsage(1.0, 10.0));
		
		tg = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.RdsFull), Operation.getOperation("CreateDBInstance"), UsageType.getUsageType("RDS:GP2-Storage", "GB"), null);
		testSerializeDeserialize(tg, new CostAndUsage(1.0, 10.0));
	}
	
	private void testSerializeDeserialize(TagGroup tg, CostAndUsage value) throws IOException, BadZone {
		DataSerializer data = new DataSerializer(tg.resourceGroup == null ? 0 : tg.resourceGroup.getUserTags().length);
		data.enableTagGroupCache(true);
		
        List<Map<TagGroup, CostAndUsage>> list = Lists.newArrayList();
        Map<TagGroup, CostAndUsage> map = DataSerializer.getCreateData(list, 0);
        map.put(tg, value);
		data.setData(list, 0);
		
		DataSerializer result = serializeDeserialize(as, ps, data);
		result.enableTagGroupCache(true);
		
		assertEquals("Wrong number of tag groups in tagGroups", 1, result.getTagGroups().size());
		assertEquals("Length of data is wrong", 1, result.getNum());
		assertEquals("Length of first num is wrong", 1, result.getData(0).size());
		assertEquals("Cost value of first num is wrong", value.cost, result.get(0, tg).cost, 0.001);
		assertEquals("Usage value of first num is wrong", value.usage, result.get(0, tg).usage, 0.001);
	}
	
	@Test
	public void testSerializeDeserializeTwice() throws IOException, BadZone {
		DataSerializer data = new DataSerializer(0);
		data.enableTagGroupCache(true);
		
		TagGroup tg = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);
        List<Map<TagGroup, CostAndUsage>> list = Lists.newArrayList();
        Map<TagGroup, CostAndUsage> map = DataSerializer.getCreateData(list, 0);
        map.put(tg, new CostAndUsage(1, 10));
		data.setData(list, 0);
		
		data = serializeDeserialize(as, ps, data);
		
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);

		list = Lists.newArrayList();
		map = DataSerializer.getCreateData(list, 0);
		map.put(tg2, new CostAndUsage(2, 20));
		data.setData(list, 1);
		
		DataSerializer result = serializeDeserialize(as, ps, data);
		result.enableTagGroupCache(true);
		
		assertEquals("Wrong number of tags in in tagGroups", 1, result.getTagGroups().size());
		assertEquals("Length of data is wrong", 2, result.getNum());
		assertEquals("Length of first num is wrong", 1, result.getData(0).size());
		assertEquals("Cost value of first num is wrong", 1.0, result.get(0, tg).cost, 0.001);
		assertEquals("Usage value of first num is wrong", 10.0, result.get(0, tg).usage, 0.001);
		assertEquals("Length of second num is wrong", 1, result.getData(1).size());
		assertEquals("Cost value of second num is wrong", 2.0, result.get(1, tg2).cost, 0.001);
		assertEquals("Usage value of second num is wrong", 20.0, result.get(1, tg2).usage, 0.001);
		assertEquals("Tags don't match", tg, tg2);
	}
	
	DataSerializer serializeDeserialize(AccountService as, ProductService ps, DataSerializer data) throws IOException, BadZone {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(output);
		
		data.serialize(out, null);
		ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
		DataInput in = new DataInputStream(input);
		data = new DataSerializer(0);
		data.enableTagGroupCache(true);
		data.deserialize(as, ps, in);
		return data;
	}
	
    public static void serialize(OutputStreamWriter out, DataSerializer data) throws IOException {
    	out.write("num,cost,usage,account,region,zone,product,operation,usageType,usageUnits,resource\n");
        Collection<TagGroup> keys = data.getTagGroups();

        for (Integer i = 0; i < data.getNum(); i++) {
            Map<TagGroup, CostAndUsage> map = data.getData(i);
            if (map.size() > 0) {
                for (TagGroup tagGroup: keys) {
                	CostAndUsage v = map.get(tagGroup);
                    out.write(i.toString() + ",");
                    out.write(v == null ? "0,0," : (Double.toString(v.cost) + "," + Double.toString(v.usage) + ","));
                    TagGroup.Serializer.serializeCsv(out, tagGroup);
                    out.write("\n");
                }
            }
        }
    }
    
    @Test
    public void testPutAll() {
    	// test the merging of two data sets.
    	DataSerializer a = new DataSerializer(0);
    	DataSerializer b = new DataSerializer(0);
    	
		TagGroup tg1 = TagGroup.getTagGroup(as.getAccountByName("Account1"), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);
		TagGroup tg2 = TagGroup.getTagGroup(as.getAccountByName("Account2"), Region.US_WEST_2, null, ps.getProduct(Product.Code.S3), Operation.getOperation("StandardStorage"), UsageType.getUsageType("TimedStorage-ByteHrs", "GB"), null);
    	
    	a.put(0, tg1, new CostAndUsage(1.0, 10.0));
    	a.put(0, tg2, new CostAndUsage(2.0, 20.0));
    	b.put(0, tg2, new CostAndUsage(4.0, 40.0));
    	a.putAll(b);
    	
    	assertEquals("TagGroup 1 cost is not correct", 1.0, a.get(0, tg1).cost, .001);
    	assertEquals("TagGroup 1 usage is not correct", 10.0, a.get(0, tg1).usage, .001);
    	assertEquals("TagGroup 2 cost is not correct", 6.0, a.get(0, tg2).cost, .001);
    	assertEquals("TagGroup 2 cost is not correct", 60.0, a.get(0, tg2).usage, .001);
    }

}
