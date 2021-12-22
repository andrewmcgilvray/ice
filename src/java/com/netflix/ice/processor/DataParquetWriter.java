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
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.common.WorkBucketConfig;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.UserTag;
import com.netflix.ice.tag.UserTagKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DataParquetWriter {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final DateTime monthDateTime;
    private final List<UserTagKey> tagKeys;
    private final Map<Product, DataSerializer> dataByProduct;
    private final WorkBucketConfig config;
    private final MessageType schema;

    public DataParquetWriter(DateTime monthDateTime, List<UserTagKey> tagKeys,
                             Map<Product, DataSerializer> dataByProduct,
                             WorkBucketConfig workBucketConfig) {
        this.monthDateTime = monthDateTime;
        this.tagKeys = tagKeys;
        this.dataByProduct = dataByProduct;
        this.config = workBucketConfig;

        // Build the Schema
        StringBuilder sb = new StringBuilder(1024);
        sb.append("message TagGroup {\n");
        // Primary dimensions
        sb.append("  required binary costType (UTF8);\n");
        sb.append("  required binary org (UTF8);\n");
        sb.append("  required binary accountId (UTF8);\n");
        sb.append("  required binary account (UTF8);\n");
        sb.append("  optional binary region (UTF8);\n");
        sb.append("  optional binary zone (UTF8);\n");
        sb.append("  optional binary product (UTF8);\n");
        sb.append("  optional binary productCode (UTF8);\n");
        sb.append("  optional binary operation (UTF8);\n");
        sb.append("  optional binary usageType (UTF8);\n");
        sb.append("  optional binary usageTypeUnit (UTF8);\n");

        for (UserTagKey key: tagKeys) {
            sb.append("  optional binary ").append(key.name).append(" (UTF8);\n");
        }

        // Time ordered cost and usage pairs
        sb.append("  required group days (LIST) {\n");
        sb.append("    repeated group list {\n");
        sb.append("      optional group element {\n");
        sb.append("        required double cost;\n");
        sb.append("        required double usage;\n");
        sb.append("      }\n");
        sb.append("    }\n");
        sb.append("  }\n");
        sb.append("}");
        this.schema = MessageTypeParser.parseMessageType(sb.toString());
    }

    class ParquetWriterWrapper implements Closeable {
        private final ParquetWriter<Group> parquetWriter;

        public ParquetWriterWrapper(final Path file) throws IOException {
            GroupWriteSupport writeSupport = new GroupWriteSupport();
            Configuration conf = new Configuration();
            GroupWriteSupport.setSchema(schema, conf);
            parquetWriter = new ParquetWriter<Group>(
                    file,
                    ParquetFileWriter.Mode.OVERWRITE,
                    writeSupport,
                    ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetWriter.DEFAULT_WRITER_VERSION,
                    conf);
            /*
            ParquetWriter<Group>.Builder parquetWriterBuilder = new ParquetWriter<>.Builder(file) {
                @Override
                protected ParquetWriter.Builder self() {
                    return this;
                }

                @Override
                protected WriteSupport<Group> getWriteSupport(Configuration conf) {
                    return writeSupport;
                }
            };

            parquetWriterBuilder.withCompressionCodec(codecName)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withConf(conf)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
            parquetWriter = parquetWriterBuilder.build();

             */
        }

        public void write(Group group) throws IOException {
            parquetWriter.write(group);
        }

        @Override
        public void close() throws IOException {
            parquetWriter.close();
        }
    }

    public void archive() throws IOException {
        String yearAndMonth = AwsUtils.monthDateFormat.print(monthDateTime);
        String suffix =  yearAndMonth + ".parquet";
        File dailyFile = new File(config.localDir, "daily_" + suffix);
        File monthlyFile = new File(config.localDir, "monthly_" + suffix);
        write(dailyFile, monthlyFile);

        if (config.workS3BucketName != null) {
            String[] ym = yearAndMonth.split("-");
            String dailyKey = "daily_parquet/" + ym[0] + "/" + ym[1] + "/" + dailyFile.getName();
            String monthlyKey = "monthly_parquet/" + ym[0] + "/" + ym[1] + "/" + monthlyFile.getName();

            upload(dailyKey, dailyFile);
            upload(monthlyKey, monthlyFile);
        }
    }

    private void upload(String key, File file) {
        logger.info(file.getName() + " uploading to s3...");
        AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix + key, file);
        logger.info(file.getName() + " uploading done.");
    }

    protected void write(File dailyFile, File monthlyFile) throws IOException {
        ParquetWriterWrapper dailyWriter = new ParquetWriterWrapper(new Path(dailyFile.getPath()));
        ParquetWriterWrapper monthlyWriter = new ParquetWriterWrapper(new Path(monthlyFile.getPath()));

        for (Product product: dataByProduct.keySet()) {
            // Skip the "null" product map that doesn't have resource tags
            if (product == null)
                continue;

            DataSerializer data = dataByProduct.get(product);
            Collection<TagGroup> tagGroups = data.getTagGroups();

            List<Map<TagGroup, DataSerializer.CostAndUsage>> daily = Lists.newArrayList();
            List<Map<TagGroup, DataSerializer.CostAndUsage>> monthly = Lists.newArrayList();

            // Aggregate
            for (int hour = 0; hour < data.getNum(); hour++) {
                Map<TagGroup, DataSerializer.CostAndUsage> cauMap = data.getData(hour);

                for (TagGroup tagGroup : tagGroups) {
                    DataSerializer.CostAndUsage cau = cauMap.get(tagGroup);
                    if (cau != null && !cau.isZero()) {
                        addValue(daily, hour / 24, tagGroup, cau);
                        addValue(monthly, 0, tagGroup, cau);
                    }
                }
            }
            writeData(dailyWriter, tagGroups, daily);
            writeData(monthlyWriter, tagGroups, monthly);
        }
        dailyWriter.close();
        monthlyWriter.close();
    }

    private void addValue(List<Map<TagGroup, DataSerializer.CostAndUsage>> list,
                          int index, TagGroup tagGroup, DataSerializer.CostAndUsage v) {
        Map<TagGroup, DataSerializer.CostAndUsage> map = DataSerializer.getCreateData(list, index);
        DataSerializer.CostAndUsage existedV = map.get(tagGroup);
        map.put(tagGroup, existedV == null ? v : existedV.add(v));
    }

    private void writeData(ParquetWriterWrapper writer, Collection<TagGroup> tagGroups, List<Map<TagGroup, DataSerializer.CostAndUsage>> data) throws IOException {
        // Write it out
        GroupFactory groupFactory = new SimpleGroupFactory(schema);

        for (TagGroup tagGroup: tagGroups) {
            Group record = groupFactory.newGroup();

            // required fields
            record.add("costType", tagGroup.costType.name);
            record.add("org", String.join("/", tagGroup.account.getParents()));
            record.add("accountId", tagGroup.account.getId());
            record.add("account", tagGroup.account.getIceName());

            // optional fields
            if (tagGroup.region != null) {
                record.add("region", tagGroup.region.name);
            }
            if (tagGroup.zone != null) {
                record.add("zone", tagGroup.zone.name);
            }
            if (tagGroup.product != null) {
                record.add("product", tagGroup.product.getIceName());
                record.add("productCode", tagGroup.product.getServiceCode());
            }
            if (tagGroup.operation != null) {
                record.add("operation", tagGroup.operation.name);
            }
            if (tagGroup.usageType != null) {
                record.add("usageType", tagGroup.usageType.name);
                record.add("usageTypeUnit", tagGroup.usageType.unit);
            }

            if (tagGroup.resourceGroup != null) {
                UserTag[] userTags = tagGroup.resourceGroup.getUserTags();
                for (int i = 0; i < userTags.length; i++) {
                    if (userTags[i] == null || userTags[i].name.isEmpty())
                        continue;
                    record.add(tagKeys.get(i).name, userTags[i].name);
                }
            }

            Group daysList = record.addGroup("days").addGroup("list");
            for (int day = 0; day < data.size(); day++) {
                Map<TagGroup, DataSerializer.CostAndUsage> cauMap = data.get(day);
                DataSerializer.CostAndUsage cau = cauMap.get(tagGroup);

                if (cau == null)
                    cau = new DataSerializer.CostAndUsage(0, 0);

                daysList.addGroup("element").append("cost", cau.cost).append("usage", cau.usage);
            }
            writer.write(record);
        }
    }
}
