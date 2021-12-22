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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.tag.*;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DataParquetWriterTest {
    private static final String tmpDir = "src/test/tmp/";
    private static final String resourcesDir = "src/test/resources/";

    protected Logger logger = LoggerFactory.getLogger(getClass());

    static private ProductService ps;
    static private AccountService as;
    static private Product s3;
    static private Account a1;

    @BeforeClass
    static public void init() {
        ps = new BasicProductService();
        as = new BasicAccountService();

        a1 = as.getAccountById("123456789012", "root");
        s3 = ps.getProduct(Product.Code.S3);
    }

    private TagGroup getTagGroup(Product.Code product, String operation, String usageType, String[] resource) throws Zone.BadZone, ResourceGroup.ResourceException {
        Product prod = ps.getProduct(product);
        return TagGroup.getTagGroup(
                CostType.recurring,
                a1,
                Region.US_EAST_1, Region.US_EAST_1.getZone("us-east-1a"),
                prod,
                Operation.getOperation(operation),
                UsageType.getUsageType(usageType, ""),
                resource == null ? null : ResourceGroup.getResourceGroup(resource));
    }

    static class Data {
        static class TestDataSerializer extends DataSerializer {
            public TestDataSerializer(int numUserTags) {
                super(numUserTags);
                enableTagGroupCache(true);
            }

            void setData(TagGroup tg, Double cost, Double usage, int i) {
                if (i >= data.size()) {
                    getCreateData(i);
                }
                data.get(i).put(tg, new CostAndUsage(cost, usage));
                this.tagGroups.add(tg);
            }
        }
        public Map<Product, DataSerializer> dataByProduct;
        public int numUserTags;

        public Data(int numUserTags) {
            this.dataByProduct = Maps.newHashMap();
            this.numUserTags = numUserTags;
        }

        public void add(TagGroup tg, Double cost, Double usage, Double cost2, Double usage2) {
            if (!dataByProduct.containsKey(tg.product))
                dataByProduct.put(tg.product, new DataParquetWriterTest.Data.TestDataSerializer(numUserTags));

            ((DataParquetWriterTest.Data.TestDataSerializer)dataByProduct.get(tg.product)).setData(tg, cost, usage, 0);
            ((DataParquetWriterTest.Data.TestDataSerializer)dataByProduct.get(tg.product)).setData(tg, cost2, usage2, 24);
        }
    }

    @Test
    public void name() {
    }

    @Test
    public void testWrite() throws Exception {
        DataParquetWriterTest.Data data = new DataParquetWriterTest.Data(2);

        TagGroup tg1 = getTagGroup(s3.getCode(), "CopyObject", "Requests-Tier1", new String[]{"", ""});
        TagGroup tg2 = getTagGroup(s3.getCode(), "GetObject", "Requests-Tier2", new String[]{"foo", "bar"});
        data.add(tg1, 1.11, 1.1, 1.22, 1.2);
        data.add(tg2, 2.11, 2.1, 2.22, 2.2);

        DateTime dt = DateTime.parse("2017-08-01T00:00:00Z");

        List<UserTagKey> tagKeys = Lists.newArrayList();
        tagKeys.add(UserTagKey.get("Tag1"));
        tagKeys.add(UserTagKey.get("Tag2"));

        File file = new File(tmpDir, "daily_2017-08.parquet");
        if (file.exists())
            file.delete();

        WorkBucketConfig workBucketConfig = new WorkBucketConfig(null, null, null, tmpDir);

        DataParquetWriter djw = new DataParquetWriter(dt, tagKeys, data.dataByProduct, workBucketConfig);
        djw.archive();

        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<Group>(new Path(file.getPath()), readSupport);

        Map<TagGroup, List<DataSerializer.CostAndUsage>> got = loadData(reader, tagKeys);
        assertEquals("wrong number of records", 2, got.size());

        List<DataSerializer.CostAndUsage> cauList = got.get(tg1);
        assertNotNull(cauList);
        assertEquals("tg1[0] cost incorrect", 1.11, cauList.get(0).cost, 0.001);
        assertEquals("tg1[0] usage incorrect", 1.1, cauList.get(0).usage, 0.001);
        assertEquals("tg1[1] cost incorrect", 1.22, cauList.get(1).cost, 0.001);
        assertEquals("tg1[1] usage incorrect", 1.2, cauList.get(1).usage, 0.001);
        assertNotNull(cauList = got.get(tg2));
        assertEquals("tg2[0] cost incorrect", 2.11, cauList.get(0).cost, 0.001);
        assertEquals("tg2[0] usage incorrect", 2.1, cauList.get(0).usage, 0.001);
        assertEquals("tg2[1] cost incorrect", 2.22, cauList.get(1).cost, 0.001);
        assertEquals("tg2[1] usage incorrect", 2.2, cauList.get(1).usage, 0.001);

    }

    Map<TagGroup, List<DataSerializer.CostAndUsage>> loadData(ParquetReader<Group> reader, List<UserTagKey> tagKeys) throws Exception {
        Map<TagGroup, List<DataSerializer.CostAndUsage>> data = Maps.newHashMap();

        while (true) {
            Group row = reader.read();
            if (row == null)
                break;

            String[] resourceGroup = new String[tagKeys.size()];
            for (int i = 0; i < tagKeys.size(); i++) {
                resourceGroup[i] = row.getFieldRepetitionCount(tagKeys.get(i).name) == 0 ? null : row.getString(tagKeys.get(i).name, 0);
            }
            TagGroup tg = TagGroup.getTagGroup(
                    row.getString("costType", 0),
                    row.getString("account", 0),
                    row.getString("region", 0),
                    row.getString("zone", 0),
                    row.getString("productCode", 0),
                    row.getString("operation", 0),
                    row.getString("usageType", 0), row.getString("usageTypeUnit", 0),
                    resourceGroup,
                    as, ps
                    );

            Group daysList = row.getGroup("days", 0);
            int days = daysList.getFieldRepetitionCount("list");
            List<DataSerializer.CostAndUsage> cauList = Lists.newArrayListWithCapacity(days);
            for (int day = 0; day < days; day++) {
                Group element = daysList.getGroup("list", day).getGroup("element", 0);
                DataSerializer.CostAndUsage cau = new DataSerializer.CostAndUsage(
                        element.getDouble("cost", 0),
                        element.getDouble("usage", 0));
                cauList.add(cau);
            }
            data.put(tg, cauList);
        }

        return data;
    }

}
