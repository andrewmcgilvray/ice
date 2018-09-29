package com.netflix.ice.common;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.Zone;

public class Instance {
    protected static Logger logger = LoggerFactory.getLogger(Instance.class);

    public final String id;
	public final String type;
	public final Account account;
	public final Region region;
	public final Zone zone;
	public final Product product;
	public final Map<String, String> tags;
	
	public final long startMillis; // start time of billing lineitem record. Not serialized.
	
	public Instance(String id, String type, Account account, Region region, Zone zone, Product product, Map<String, String> tags, long startMillis) {
		this.id = id;
		this.type = type;
		this.account = account;
		this.region = region;
		this.zone = zone;
		this.product = product;
		this.tags = tags;
		this.startMillis = startMillis;
	}
	
	public static String header() {
		return "InstanceID,InstanceType,AccountId,AccountName,Region,Zone,Product,Tags\n";
	}
	
	public String serialize() {
		String[] cols = new String[]{
			id,
			type,
			account.id,
			account.name,
			region.toString(),
			zone == null ? "" : zone.toString(),
			product.name,
			resourceTagsToString(tags),
		};
		return StringUtils.join(cols, ",") + "\n";
	}
	
	private String resourceTagsToString(Map<String, String> tags) {
    	StringBuilder sb = new StringBuilder();
    	boolean first = true;
    	for (Entry<String, String> entry: tags.entrySet()) {
    		String tag = entry.getKey();
    		if (tag.startsWith("user:"))
    			tag = tag.substring("user:".length());
    		sb.append((first ? "" : "|") + tag + "=" + entry.getValue());
    		first = false;
    	}
    	String ret = sb.toString();
    	if (ret.contains(","))
    		ret = "\"" + ret + "\"";
    	return ret;
	}
	
	public static Instance deserialize(String in, AccountService accountService, ProductService productService) {
		// remove the newline before splitting
        String[] values = in.trim().split(",");
        
        String id = values[0];
        String type = values[1];
        Account account = accountService.getAccountById(values[2]);
        Region region = Region.getRegionByName(values[4]);
        Zone zone = (values.length > 5 && !values[5].isEmpty()) ? Zone.getZone(values[5]) : null;
        Product product = productService.getProductByName(values[6]);

        final int TAGS_INDEX = 7;
        Map<String, String> tags = Maps.newHashMap();
        if (values.length > TAGS_INDEX) {
	        if (values.length > TAGS_INDEX + 1) {
	            StringBuilder tagsStr = new StringBuilder();
	            for (int i = TAGS_INDEX; i < values.length; i++)
	            	tagsStr.append(values[i] + ",");
	            // remove last comma
	            tagsStr.deleteCharAt(tagsStr.length() - 1);
	        	values[TAGS_INDEX] = tagsStr.toString();
	        }
	        // Remove quotes from tags if present
	        if (values[TAGS_INDEX].startsWith("\""))
	        	values[TAGS_INDEX] = values[TAGS_INDEX].substring(1, values[TAGS_INDEX].length() - 1);
	        
	        tags = parseResourceTags(values[TAGS_INDEX]);
        }
        
    	return new Instance(id, type, account, region, zone, product, tags, 0);
	}
	

	private static Map<String, String> parseResourceTags(String in) {
		Map<String, String> tags = Maps.newHashMap();
		String[] pairs = in.split("\\|");
		if (pairs[0].isEmpty())
			return tags;
		
		for (String tag: pairs) {
			// split on first "="
			int i = tag.indexOf("=");
			if (i <= 0) {
				logger.error("Bad tag: " + tag);
			}
			String key = tag.substring(0, i);
			tags.put(key, tag.substring(i + 1));
		}
		return tags;
	}
}