package com.netflix.ice.processor;

import com.google.common.collect.Lists;
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

    private ParquetWriterWrapper writer;
    private final List<UserTagKey> tagKeys;
    private final Map<Product, DataSerializer> dataByProduct;
    private final File file;
    private final WorkBucketConfig config;
    private final MessageType schema;

    public DataParquetWriter(String name, List<UserTagKey> tagKeys,
                             Map<Product, DataSerializer> dataByProduct,
                             WorkBucketConfig workBucketConfig) {
        this.tagKeys = tagKeys;
        this.dataByProduct = dataByProduct;
        this.config = workBucketConfig;
        this.file = new File(config.localDir, name);

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

        // User tags
        sb.append("  required group tags {\n");
        for (UserTagKey key: tagKeys) {
            sb.append("    optional binary ").append(key.name).append(" (UTF8);\n");
        }
        sb.append("  }\n");

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

    static class ParquetWriterWrapper implements Closeable {
        private final ParquetWriter<Group> parquetWriter;

        public ParquetWriterWrapper(
                final Path file,
                final WriteSupport<Group> writeSupport,
                final CompressionCodecName codecName,
                final Configuration conf
        ) throws IOException {
            parquetWriter = new ParquetWriter<Group>(
                    file,
                    ParquetFileWriter.Mode.OVERWRITE,
                    writeSupport,
                    codecName,
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
            //logger.info("Write group: " + group.toString());
            parquetWriter.write(group);
        }

        @Override
        public void close() throws IOException {
            parquetWriter.close();
        }
    }

    public void archive() throws IOException {
        Configuration conf = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, conf);
        writer = new ParquetWriterWrapper(new Path(file.getPath()), writeSupport,
                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                conf);

        write();

        writer.close();
        if (config.workS3BucketName != null) {
            logger.info(file.getName() + " uploading to s3...");
            AwsUtils.upload(config.workS3BucketName, config.workS3BucketPrefix + file.getName(), file);
            logger.info(file.getName() + " uploading done.");
        }
    }

    protected void write() throws IOException {
        for (Product product: dataByProduct.keySet()) {
            // Skip the "null" product map that doesn't have resource tags
            if (product == null)
                continue;

            writeDaily(dataByProduct.get(product));
        }
    }

    private void writeDaily(DataSerializer data) throws IOException {
        List<Map<TagGroup, DataSerializer.CostAndUsage>> daily = Lists.newArrayList();

        Collection<TagGroup> tagGroups = data.getTagGroups();

        // Aggregate
        for (int hour = 0; hour < data.getNum(); hour++) {
            Map<TagGroup, DataSerializer.CostAndUsage> cauMap = data.getData(hour);

            for (TagGroup tagGroup: tagGroups) {
                DataSerializer.CostAndUsage cau = cauMap.get(tagGroup);
                if (cau != null && !cau.isZero())
                    addValue(daily, hour/24, tagGroup, cau);
            }
        }

        // Write it out
        GroupFactory groupFactory = new SimpleGroupFactory(schema);

        for (TagGroup tagGroup: tagGroups) {
            Group record = groupFactory.newGroup();
            if (tagGroup.costType != null) {
                record.add("costType", tagGroup.costType.name);
            }
            if (tagGroup.account != null) {
                record.add("org", String.join("/", tagGroup.account.getParents()));
                record.add("accountId", tagGroup.account.getId());
                record.add("account", tagGroup.account.getIceName());
            }
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
                Group tags = record.addGroup("tags");
                UserTag[] userTags = tagGroup.resourceGroup.getUserTags();
                for (int i = 0; i < userTags.length; i++) {
                    if (userTags[i] == null || userTags[i].name.isEmpty())
                        continue;
                    tags.add(tagKeys.get(i).name, userTags[i].name);
                }
            }

            Group daysList = record.addGroup("days").addGroup("list");
            for (int day = 0; day < daily.size(); day++) {
                Map<TagGroup, DataSerializer.CostAndUsage> cauMap = daily.get(day);
                DataSerializer.CostAndUsage cau = cauMap.get(tagGroup);

                if (cau == null)
                    cau = new DataSerializer.CostAndUsage(0, 0);

                daysList.addGroup("element").append("cost", cau.cost).append("usage", cau.usage);
            }
            writer.write(record);
        }
    }

    private void addValue(List<Map<TagGroup, DataSerializer.CostAndUsage>> list, int index, TagGroup tagGroup, DataSerializer.CostAndUsage v) {
        Map<TagGroup, DataSerializer.CostAndUsage> map = DataSerializer.getCreateData(list, index);
        DataSerializer.CostAndUsage existedV = map.get(tagGroup);
        map.put(tagGroup, existedV == null ? v : existedV.add(v));
    }
}
