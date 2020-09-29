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
package com.netflix.ice.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.tag.*;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.Zone.BadZone;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TagGroup implements Comparable<TagGroup>, Serializable {
	private static final long serialVersionUID = 3L;
    //private final Logger logger = LoggerFactory.getLogger(getClass());
	
	public final Account account;
    public final Product product;
    public final Operation operation;
    public final UsageType usageType;
    public final Region region;
    public final Zone zone;
    public final ResourceGroup resourceGroup;
    
    private final int hashcode;
    
    protected TagGroup(Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup) {
        this.account = account;
        this.region = region;
        this.zone = zone;
        this.product = product;
        this.operation = operation;
        this.usageType = usageType;
        this.resourceGroup = resourceGroup;
        
        this.hashcode = genHashCode();
    }

    @Override
    public String toString() {
        return "\"" + account + "\",\"" + region + "\",\"" + zone + "\",\"" + product + "\",\"" + operation + "\",\"" + usageType + "\",\"" + resourceGroup + "\"";
    }

    public int compareTo(TagGroup t) {
    	if (this == t)
    		return 0;
        int result = this.account == t.account ? 0 : (this.account == null ? 1 : (t.account == null ? -1 : this.account.compareTo(t.account)));
        if (result != 0)
            return result;
        result = this.region == t.region ? 0 : (this.region == null ? 1 : (t.region == null ? -1 : this.region.compareTo(t.region)));
        if (result != 0)
            return result;
        result = this.zone == t.zone ? 0 : (this.zone == null ? 1 : (t.zone == null ? -1 : t.zone.compareTo(this.zone)));
        if (result != 0)
            return result;
        result = this.product == t.product ? 0 : (this.product == null ? 1 : (t.product == null ? -1 : this.product.compareTo(t.product)));
        if (result != 0)
            return result;
        result = this.operation == t.operation ? 0 : (this.operation == null ? 1 : (t.operation == null ? -1 : this.operation.compareTo(t.operation)));
        if (result != 0)
            return result;
        result = this.usageType == t.usageType ? 0 : (this.usageType == null ? 1 : (t.usageType == null ? -1 : this.usageType.compareTo(t.usageType)));
        if (result != 0)
            return result;
        result = this.resourceGroup == t.resourceGroup ? 0 : (this.resourceGroup == null ? 1 : (t.resourceGroup == null ? -1 : t.resourceGroup.compareTo(this.resourceGroup)));
        if (result != 0)
            return result;
        // Handle any subclass extensions
        return compareKey().compareTo(t.compareKey());
    }
    
    public String compareKey() {
    	return "";
    }

    @Override
    public boolean equals(Object o) {
    	if (this == o)
    		return true;
        if (o == null)
            return false;
        TagGroup other = (TagGroup)o;

        boolean match = 
                this.zone == other.zone &&
                this.account == other.account &&
                this.region == other.region &&
                this.product == other.product &&
                this.operation == other.operation &&
                this.usageType == other.usageType &&
                this.resourceGroup == other.resourceGroup;
        return match;
    }

    @Override
    public int hashCode() {
    	return hashcode;
    }

    private int genHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (zone != null ? zone.hashCode() : 0);
        result = prime * result + (account != null ? account.hashCode() : 0);
        result = prime * result + (region != null ? region.hashCode() : 0);
        result = prime * result + (product != null ? product.hashCode() : 0);
        result = prime * result + (operation != null ? operation.hashCode() : 0);
        result = prime * result + (usageType != null ? usageType.hashCode() : 0);
        result = prime * result + (resourceGroup != null ? resourceGroup.hashCode() : 0);

        return result;
    }

    private static Map<TagGroup, TagGroup> tagGroups = Maps.newConcurrentMap();

    public static TagGroup getTagGroup(
    		String account, String region, String zone, String product, String operation, String usageTypeName, String usageTypeUnit,
    		String[] resourceGroup, AccountService accountService, ProductService productService) throws BadZone, ResourceException {
        Region r = Region.getRegionByName(region);
    	return getTagGroup(
    		accountService.getAccountByName(account),
        	r, StringUtils.isEmpty(zone) ? null : r.getZone(zone),
        	productService.getProductByServiceCode(product),
        	Operation.getOperation(operation),
            UsageType.getUsageType(usageTypeName, usageTypeUnit),
            ResourceGroup.getResourceGroup(resourceGroup));   	
    }
    
    public static TagGroup getTagGroup(Account account, Region region, Zone zone, Product product, Operation operation, UsageType usageType, ResourceGroup resourceGroup) {
        TagGroup newOne = new TagGroup(account, region, zone, product, operation, usageType, resourceGroup);
        TagGroup oldOne = tagGroups.get(newOne);
        if (oldOne != null) {
            return oldOne;
        }
        else {
            tagGroups.put(newOne, newOne);
            return newOne;
        }
    }
    
    public TagGroup withOperation(Operation op) {
    	return getTagGroup(account, region, zone, product, op, usageType, resourceGroup);
    }
    
    public TagGroup withResourceGroup(ResourceGroup rg) {
    	return getTagGroup(account, region, zone, product, operation, usageType, rg);
    }

    public static class Serializer {

        public static void serializeTagGroups(DataOutput out, TreeMap<Long, Collection<TagGroup>> tagGroups) throws IOException {
            out.writeInt(tagGroups.size());
            for (Long monthMilli: tagGroups.keySet()) {
                out.writeLong(monthMilli);
                Collection<TagGroup> keys = tagGroups.get(monthMilli);
                out.writeInt(keys.size());
                for (TagGroup tagGroup: keys) {
                    serialize(out, tagGroup);
                }
            }
        }

        public static void serialize(DataOutput out, TagGroup tagGroup) throws IOException {
            out.writeUTF(tagGroup.account == null ? "" : tagGroup.account.getId());
            out.writeUTF(tagGroup.region == null ? "" : tagGroup.region.toString());
            out.writeUTF(tagGroup.zone == null ? "" : tagGroup.zone.toString());
            out.writeUTF(tagGroup.product == null ? "" : tagGroup.product.getServiceCode());
            out.writeUTF(tagGroup.operation == null ? "" : tagGroup.operation.toString());
            UsageType.serialize(out, tagGroup.usageType);
            ResourceGroup.serialize(out,  tagGroup.resourceGroup);
        }
        
        public static void serializeCsvHeader(OutputStreamWriter out, String resourceGroupHeader) throws IOException {
        	out.write("account,region,zone,product,operation,");
        	UsageType.serializeCsvHeader(out);
        	if (!StringUtils.isEmpty(resourceGroupHeader))
        		out.write(resourceGroupHeader);
        }

        public static void serializeCsv(Writer out, TagGroup tagGroup) throws IOException {
            out.write((tagGroup.account == null ? "" : tagGroup.account.getId()) + ",");
            out.write((tagGroup.region == null ? "" : tagGroup.region.toString()) + ",");
            out.write((tagGroup.zone == null ? "" : tagGroup.zone.toString()) + ",");
            out.write((tagGroup.product == null ? "" : tagGroup.product.getServiceCode()) + ",");
            out.write((tagGroup.operation == null ? "" : tagGroup.operation.toString()) + ",");
            UsageType.serializeCsv(out, tagGroup.usageType);
            if (tagGroup.resourceGroup != null) {
            	out.write(",");
            	ResourceGroup.serializeCsv(out, tagGroup.resourceGroup);
            }
        }

        public static TreeMap<Long, Collection<TagGroup>> deserializeTagGroups(AccountService accountService, ProductService productService, int numUserTags, DataInput in) throws IOException, BadZone {
            int numCollections = in.readInt();
            TreeMap<Long, Collection<TagGroup>> result = Maps.newTreeMap();
            for (int i = 0; i < numCollections; i++) {
                long monthMilli = in.readLong();
                int numKeys = in.readInt();
                List<TagGroup> keys = Lists.newArrayList();
                for (int j = 0; j < numKeys; j++) {
                    keys.add(deserialize(accountService, productService, numUserTags, in));
                }
                result.put(monthMilli, keys);
            }

            return result;
        }

        public static TagGroup deserialize(AccountService accountService, ProductService productService, int numUserTags, DataInput in) throws IOException, BadZone {
        	String v = in.readUTF();
            Account account = v.isEmpty() ? null : accountService.getAccountById(v);
            
            v = in.readUTF();
            Region region = v.isEmpty() ? null : Region.getRegionByName(v);
            
            v = in.readUTF();
            Zone zone = v.isEmpty() ? null : region.getZone(v);
            
            v = in.readUTF();
            Product product =  v.isEmpty() ? null : productService.getProductByServiceCode(v);
            
            v = in.readUTF();
            Operation operation = v.isEmpty() ? null : Operation.deserializeOperation(v);
            
            UsageType usageType = UsageType.deserialize(in);
            ResourceGroup resourceGroup = ResourceGroup.deserialize(in, numUserTags);

            return TagGroup.getTagGroup(account, region, zone, product, operation, usageType, resourceGroup);
        }
                
        // Serialize to CSV for general debugging
        public static void serializeTagGroupsCsv(DataOutput out, TreeMap<Long, Collection<TagGroup>> tagGroups) throws IOException {
            out.writeChars("Month,Account,Region,Zone,Product,Operation,UsageType,UsageTypeUnit,ResourceGroup\n");
            DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC);

            for (Long monthMilli: tagGroups.keySet()) {
                Collection<TagGroup> keys = tagGroups.get(monthMilli);
                for (TagGroup tagGroup: keys) {
                	StringBuilder sb = new StringBuilder(256);
                	sb.append(dateFormatter.print(monthMilli));
                	sb.append(",");
                	sb.append(tagGroup.account == null ? "" : tagGroup.account.toString());
                	sb.append(",");
                	sb.append(tagGroup.region == null ? "" : tagGroup.region.toString());
                	sb.append(",");
                	sb.append(tagGroup.zone == null ? "" : tagGroup.zone.toString());
                	sb.append(",");
                	sb.append(tagGroup.product == null ? "" : tagGroup.product.getServiceCode());
                	sb.append(",");
                	sb.append(tagGroup.operation == null ? "" : tagGroup.operation.toString());
                	sb.append(",");
                	sb.append(tagGroup.usageType == null ? "" : tagGroup.usageType.name);
                	sb.append(",");
                	sb.append(tagGroup.usageType == null ? "" : tagGroup.usageType.unit);
                	sb.append(",");
                    sb.append(tagGroup.resourceGroup == null ? "" : tagGroup.resourceGroup.toString());
                    sb.append("\n");
                    
                	out.writeChars(sb.toString());
                }
            }
        }
    }
}
