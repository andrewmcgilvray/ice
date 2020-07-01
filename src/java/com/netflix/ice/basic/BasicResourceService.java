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
package com.netflix.ice.basic;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazonaws.services.ec2.model.Tag;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagConfig;
import com.netflix.ice.common.TagMappings;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UserTagKey;

public class BasicResourceService extends ResourceService {
    private final static Logger logger = LoggerFactory.getLogger(BasicResourceService.class);

    protected final List<String> customTags;
    private final List<UserTagKey> userTagKeys;
    private final boolean includeReservationIds;
    
    
    // Map of tags where each tag has a list of aliases. Outer key is the payerAccountId.
    private Map<String, Map<String, TagConfig>> tagConfigs;
    
    // Map of tag values to canonical name. All keys are lower case.
    // Maps are nested by Payer Account ID, Tag Key, then Value
    private Map<String, Map<String, Map<String, String>>> tagValuesInverted;
    
    // Map containing the lineItem column indeces that match the canonical tag keys specified by CustomTags
    // Key is the Custom Tag name (without the "user:" prefix). First index in the list is always the exact
    // custom tag name match if present.
    private Map<String, List<Integer>> tagLineItemIndeces;
    
    private final Map<String, Integer> tagResourceGroupIndeces;
    
    private static final String USER_TAG_PREFIX = "user:";
    private static final String AWS_TAG_PREFIX = "aws:";
    private static final String reservationIdsKeyName = "RI/SP ID";
    private static final String defaultTagSeparator = "/";
    private static final String defaultTagEffectiveDateSeparator = "=";
    private static final String suspend = "<suspend>";
    
    /**
     *  Map containing values to assign to destination tags based on a match with a value
     *  in a source tag. These are returned if the requested resource doesn't have a tag value.
     *  Primary map key is the payer account ID, secondary map key is the destination tag key.
     *  
     *  The full data structure in YML notation looks like this:
     *  
     *  <pre>
     *  mappedTags:
     *    payerAcctId:
     *      destTagKey:
     *        include: []
     *        exclude: []
     *        start: YYYY-MM
     *        maps:
     *          srcTagIndex:
     *            srcTagValue: destTagValue -or- "<suspend>"
     *      destTagKey2:
     *        ...
     *      destTagKey3:
     *        ...
     *    payerAcctId2:
     *      ...
     *  </pre>
     */
    private Map<String, Map<String, Map<Long, List<MappedTags>>>> mappedTags;
    
    private class MappedTags {
    	Map<Integer, Map<String, String>> maps; // Primary map key is source tag index, secondary map key is the source tag value
    	List<String> include;
    	List<String> exclude;
    	Long startMillis;
    	
    	public MappedTags(TagMappings config) {
			maps = Maps.newHashMap();
			for (String mappedValue: config.maps.keySet()) {
				Map<String, List<String>> configValueMaps = config.maps.get(mappedValue);
				for (String sourceTag: configValueMaps.keySet()) {
					Integer sourceTagIndex = tagResourceGroupIndeces.get(sourceTag);
					if (sourceTagIndex == null) {
						logger.error("Tag mapping rule references invalid source tag: " + sourceTag);
						continue;
					}
					Map<String, String> mappedValues = maps.get(sourceTagIndex);
					if (mappedValues == null) {
						mappedValues = Maps.newHashMap();
						maps.put(sourceTagIndex, mappedValues);
					}
					for (String target: configValueMaps.get(sourceTag)) {
						mappedValues.put(target.toLowerCase(), mappedValue);
					}
				}
			}
			// Add the suspended source tag values
			if (config.suspend != null) {
				for (String sourceTag: config.suspend.keySet()) {
					Integer sourceTagIndex = tagResourceGroupIndeces.get(sourceTag);
					if (sourceTagIndex == null) {
						logger.error("Tag mapping rule references invalid source tag: " + sourceTag);
						continue;
					}
					Map<String, String> mappedValues = maps.get(sourceTagIndex);
					if (mappedValues == null) {
						mappedValues = Maps.newHashMap();
						maps.put(sourceTagIndex, mappedValues);
					}
					for (String target: config.suspend.get(sourceTag)) {
						mappedValues.put(target.toLowerCase(), suspend);
					}
				}
			}
			include = config.include;
			if (include == null)
				include = Lists.newArrayList();
			exclude = config.exclude;
			if (exclude == null)
				exclude = Lists.newArrayList();
			startMillis = config.start == null || config.start.isEmpty() ? 0 : new DateTime(config.start, DateTimeZone.UTC).getMillis();
    	}
    }

    // Map of default user tag values for each account. These are returned if the requested resource doesn't
    // have a tag value nor a mapped value. Outer key is the account ID, inner map key is the tag name.
    private Map<String, Map<String, DefaultTag>> defaultTags;
    
    private class DefaultTag {
    	private class DateValue {
    		public long startMillis;
    		public String value;
    		
    		DateValue(long startMillis, String value) {
    			this.startMillis = startMillis;
    			this.value = value;
    		}
    	}
    	private List<DateValue> timeOrderedValues;
    	
    	DefaultTag(String config) {
    		Map<Long, String> sortedMap = Maps.newTreeMap();
    		String[] dateValues = config.split(defaultTagSeparator);
    		for (String dv: dateValues) {
    			String[] parts = dv.split(defaultTagEffectiveDateSeparator);
    			if (dv.contains(defaultTagEffectiveDateSeparator)) {
    				// If only one part, it's the start time and value should be empty
        			sortedMap.put(new DateTime(parts[0], DateTimeZone.UTC).getMillis(), parts.length < 2 ? "" : parts[1]);    				
    			}
    			else {
    				// If only one part, it's the value that starts at time 0
    				sortedMap.put(parts.length < 2 ? 0 : new DateTime(parts[0], DateTimeZone.UTC).getMillis(), parts[parts.length < 2 ? 0 : 1]);
    			}
    		}
    		timeOrderedValues = Lists.newArrayList();
    		for (Long start: sortedMap.keySet())
    			timeOrderedValues.add(new DateValue(start, sortedMap.get(start)));
    	}
    	
    	String getValue(long startMillis) {
    		String value = null;
    		for (DateValue dv: timeOrderedValues) {
    			if (dv.startMillis > startMillis)
    				break;
    			value = dv.value;
    		}
    		return value;
    	}
    }

    public BasicResourceService(ProductService productService, String[] customTags, boolean includeReservationIds) {
		super();
		this.includeReservationIds = includeReservationIds;
		this.customTags = Lists.newArrayList(customTags);
		if (includeReservationIds)
			this.customTags.add(reservationIdsKeyName);
		this.tagValuesInverted = Maps.newHashMap();
		this.tagResourceGroupIndeces = Maps.newHashMap();
		for (int i = 0; i < customTags.length; i++)
			tagResourceGroupIndeces.put(customTags[i], i);
				
		userTagKeys = Lists.newArrayList();
		for (String tag: this.customTags) {
			if (!tag.isEmpty())
				userTagKeys.add(UserTagKey.get(tag));
		}
		
		this.defaultTags = Maps.newHashMap();
		this.tagConfigs = Maps.newHashMap();
		this.tagValuesInverted = Maps.newHashMap();
		this.mappedTags = Maps.newHashMap();
	}
    
    @Override
    public Map<String, Map<String, TagConfig>> getTagConfigs() {
    	return tagConfigs;
    }

    @Override
    public void setTagConfigs(String payerAccountId, List<TagConfig> tagConfigs) {    	
    	if (tagConfigs == null) {
    		// Remove existing configs and indeces
    		this.tagConfigs.remove(payerAccountId);
    		this.tagValuesInverted.remove(payerAccountId);
    		return;
    	}
    	
    	Map<String, TagConfig> configs = Maps.newHashMap();
    	for (TagConfig config: tagConfigs) {
    		if (!customTags.contains(config.name)) {
    			logger.warn("Ignoring configurations for tag \"" + config.name + "\" from payer account " + payerAccountId + ", not in customTags list.");
    			continue;
    		}
    		
    		configs.put(config.name, config);
    		
        	// Add any display aliases to the user tags
    		if (config.displayAliases != null) {
				UserTagKey tagKey = UserTagKey.get(config.name);
				tagKey.addAllAliases(config.displayAliases);
    		}
    	}
    	this.tagConfigs.put(payerAccountId, configs);
    	
    	
    	// Create inverted indexes for each of the tag value alias sets
		Map<String, Map<String, String>> indeces = Maps.newHashMap();
		for (TagConfig config: configs.values()) {
			if (config.values == null || config.values.isEmpty())
				continue;
			
			Map<String, String> invertedIndex = Maps.newConcurrentMap();
			for (Entry<String, List<String>> entry: config.values.entrySet()) {			
				for (String val: entry.getValue()) {
			    	// key is all lower case and strip out all whitespace
					invertedIndex.put(val.toLowerCase().replace(" ", ""), entry.getKey());
				}
				// Handle upper/lower case differences of key and remove any whitespace
				invertedIndex.put(entry.getKey().toLowerCase().replace(" ", ""), entry.getKey());
			}
			indeces.put(config.name, invertedIndex);
		}
		this.tagValuesInverted.put(payerAccountId, indeces);
		
		// Create the maps setting tags based on the values of other tags
		Map<String, Map<Long, List<MappedTags>>> mapped = Maps.newHashMap();
		for (TagConfig config: configs.values()) {
			if (config.mapped == null || config.mapped.isEmpty())
				continue;
			Map<Long, List<MappedTags>> mappedTags = Maps.newTreeMap();
			for (TagMappings m: config.mapped) {
				MappedTags mt = new MappedTags(m);
				List<MappedTags> l = mappedTags.get(mt.startMillis);
				if (l == null) {
					l = Lists.newArrayList();
					mappedTags.put(mt.startMillis, l);
				}
				l.add(mt);
			}
			mapped.put(config.name, mappedTags);			
		}
		this.mappedTags.put(payerAccountId, mapped);
    }

	@Override
    public void init() {		
    }
	
	@Override
	public List<String> getCustomTags() {
		return customTags;
	}
	
	@Override
	public List<UserTagKey> getUserTagKeys() {
		return userTagKeys;
	}
	
	@Override
	public int getUserTagIndex(String tag) {
		return tagResourceGroupIndeces.get(tag);
	}

    @Override
    public ResourceGroup getResourceGroup(Account account, Region region, Product product, LineItem lineItem, long millisStart) {
    	if (customTags.size() == 0)
    		return null;
    	
        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.size()];
       	for (int i = 0; i < customTags.size(); i++) {
       		tags[i] = getUserTagValue(lineItem, customTags.get(i));
       	}
       	
       	for (int i = 0; i < customTags.size(); i++) {
       		String v = tags[i];
        	if (v == null || v.isEmpty())
        		v = getMappedUserTagValue(account, lineItem.getPayerAccountId(), customTags.get(i), tags, millisStart);
        	if (v == null || v.isEmpty())
        		v = getDefaultUserTagValue(account, customTags.get(i), millisStart);
        	if (v == null)
        		v = ""; // never return null entries
        	tags[i] = v;
        }
		try {
			// We never use null entries, so should never throw
			return ResourceGroup.getResourceGroup(tags);
		} catch (ResourceException e) {
			logger.error("Error creating resource group from user tags in line item" + e);
		}
		return null;
    }
    
    @Override
    public ResourceGroup getResourceGroup(Account account, Product product, List<Tag> reservedInstanceTags, long millisStart) {
    	if (customTags.size() == 0)
    		return null;
    	
        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.size()];
       	for (int i = 0; i < customTags.size(); i++) {
           	String v = null;
   			// find first matching key with a legitimate value
       		for (Tag riTag: reservedInstanceTags) {
       			if (riTag.getKey().toLowerCase().equals(customTags.get(i).toLowerCase())) {
       				v = riTag.getValue();
       				if (v != null && !v.isEmpty())
       					break;
       			}
       		}
        	if (v == null || v.isEmpty())
        		v = getDefaultUserTagValue(account, customTags.get(i), millisStart);
        	if (v == null)
        		v = ""; // never return null entries
        	tags[i] = v;
       	}
		try {
			// We never use null entries, so should never throw
			return ResourceGroup.getResourceGroup(tags);
		} catch (ResourceException e) {
			logger.error("Error creating resource group from user tags in line item" + e);
		}
		return null;
    }
    
    @Override
    public void putDefaultTags(String accountId, Map<String, String> tags) {
    	Map<String, DefaultTag> defaults = Maps.newHashMap();
    	for (String key: tags.keySet())
    		defaults.put(key, new DefaultTag(tags.get(key)));
    	defaultTags.put(accountId, defaults);
    }
    
    private String getDefaultUserTagValue(Account account, String tagKey, long startMillis) {
    	// return the default user tag value for the specified account if there is one.
    	Map<String, DefaultTag> defaults = defaultTags.get(account.getId());
    	DefaultTag dt = defaults == null ? null : defaults.get(tagKey);
    	return dt == null ? null : dt.getValue(startMillis);
    }
    
    private String getMappedUserTagValue(Account account, String payerAccount, String tag, String[] tags, long startMillis) {
    	// return the user tag value for the specified account if there is a mapping configured.
    	Map<String, Map<Long, List<MappedTags>>> mappedTagsForPayerAccount = mappedTags.get(payerAccount);
    	if (mappedTagsForPayerAccount == null)
    		return null;
    	
    	// Get the time-ordered values
    	Map<Long, List<MappedTags>> mappedTagsMap = mappedTagsForPayerAccount.get(tag);
    	Collection<List<MappedTags>> timeOrderedListsOfMappedTags = mappedTagsMap == null ? null : mappedTagsMap.values();
    	if (timeOrderedListsOfMappedTags == null)
    		return null;
    	
    	String value = null;
    	
    	for (List<MappedTags> mappedTagList: timeOrderedListsOfMappedTags) {
    		for (MappedTags mt: mappedTagList) {
	    		if (startMillis < mt.startMillis)
	    			break; // Remaining rules are not in effect yet
	    		
	        	Map<Integer, Map<String, String>> sourceTags = mt.maps;
	        	if (sourceTags == null)
	        		continue;
	        	
	        	// If we have an include filter, make sure the account is in the list
	        	if (!mt.include.isEmpty() && !mt.include.contains(account.getId()))
	        		continue;
	        	
	        	// If we have an exclude filter, make sure the account is not in the list
	        	if (!mt.exclude.isEmpty() && mt.exclude.contains(account.getId()))
	        		continue;
	        	
	        	for (Integer sourceTagIndex: sourceTags.keySet()) {
	        		String have = tags[sourceTagIndex];
	        		if (have == null)
	        			continue;
	        		Map<String, String> values = sourceTags.get(sourceTagIndex);
	        		String v = values.get(have.toLowerCase());
	        		if (v != null) {
	        			value = v.equals(suspend) ? null : v;
	        			break; // done processing this rule set. Move on to any later rule sets
	        		}
	        	}
    		}
    	}
    	
    	return value;
    }
    
    /**
     * Efficient string replace to remove spaces - much faster than String.replace()
     */
    private String stripSpaces(String s) {
    	if (s.isEmpty())
    		return "";
    	
    	char[] ca = s.toCharArray();
    	StringBuilder ret = new StringBuilder(ca.length);
    	
    	for (int i = 0; i < ca.length; i++) {
    		if (ca[i] == ' ')
    			continue;
    		ret.append(ca[i]);
    	}    	
    	return ret.toString();
    }
    
    @Override
    public String getUserTagValue(LineItem lineItem, String tag) {
    	if (includeReservationIds && tag == reservationIdsKeyName) {
    		String id = lineItem.getReservationArn();
    		if (id.isEmpty())
    			id = lineItem.getSavingsPlanArn();
    		return id;
    	}
    	
    	Map<String, Map<String, String>> indeces = tagValuesInverted.get(lineItem.getPayerAccountId());    	
    	Map<String, String> invertedIndex = indeces == null ? null : indeces.get(tag);
    	
    	// Grab the first non-empty value
    	for (int index: tagLineItemIndeces.get(tag)) {
    		if (lineItem.getResourceTagsSize() > index) {
    	    	// cut all white space from tag value
    			String val = stripSpaces(lineItem.getResourceTag(index));
    			
    			if (!StringUtils.isEmpty(val)) {
    				if (invertedIndex != null && invertedIndex.containsKey(val.toLowerCase())) {
	    				val = invertedIndex.get(val.toLowerCase());
	    			}
	    			return val;
    			}
    		}
    	}
    	return null;
    }
    
    @Override
    public boolean[] getUserTagCoverage(LineItem lineItem) {
    	boolean[] userTagCoverage = new boolean[userTagKeys.size()];
        for (int i = 0; i < userTagKeys.size(); i++) {
        	String v = getUserTagValue(lineItem, userTagKeys.get(i).name);
        	userTagCoverage[i] = !StringUtils.isEmpty(v);
        }    	
    	return userTagCoverage;
    }

    @Override
    public void commit() {

    }
    
    @Override
    public void initHeader(String[] header, String payerAccountId) {
    	tagLineItemIndeces = Maps.newHashMap();
    	Map<String, TagConfig> configs = tagConfigs.get(payerAccountId);
    	
    	/*
    	 * Create a list of billing report line item indeces for
    	 * each of the configured user tags. The list will first have
    	 * the exact match for the name if it exists in the report
    	 * followed by any case variants and specified aliases
    	 */
    	for (UserTagKey tagKey: userTagKeys) {
    		String fullTag = USER_TAG_PREFIX + tagKey.name;
    		List<Integer> indeces = Lists.newArrayList();
    		tagLineItemIndeces.put(tagKey.name, indeces);
    		
    		// First check the preferred key name
    		int index = -1;
    		for (int i = 0; i < header.length; i++) {
    			if (header[i].equals(fullTag)) {
    				index = i;
    				break;
    			}
    		}
    		if (index >= 0) {
    			indeces.add(index);
    		}
    		// Look for alternate names
            for (int i = 0; i < header.length; i++) {
            	if (i == index) {
            		continue;	// skip the exact match we handled above
            	}
            	if (fullTag.equalsIgnoreCase(header[i])) {
            		indeces.add(i);
            	}
            }
            // Look for aliases
            if (configs != null && configs.containsKey(tagKey.name)) {
            	TagConfig config = configs.get(tagKey.name);
            	if (config != null && config.aliases != null) {
	            	for (String alias: config.aliases) {
	            		String fullAlias = alias.startsWith(AWS_TAG_PREFIX) ? alias : USER_TAG_PREFIX + alias;
	                    for (int i = 0; i < header.length; i++) {
	                    	if (fullAlias.equalsIgnoreCase(header[i])) {
	                    		indeces.add(i);
	                    	}
	                    }
	            	}
            	}
            }
    	}
    }
}
