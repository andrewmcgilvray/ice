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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.ice.common.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.processor.LineItem;
import com.netflix.ice.processor.TagMappers;
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

    // Collection of tag properties for each payer account
    class PayerAccountTagProperties {
		private Map<String, TagConfig> tagConfigs;

		// Map of tag values to canonical name. All keys are lower case.
		// Maps are nested by Tag Key, then Value
		private Map<String, Map<String, String>> tagValuesInverted;

		// Map containing the lineItem column indeces that match the canonical tag keys specified by CustomTags
		// Key is the Custom Tag name (without the "user:" prefix). First index in the list is always the exact
		// custom tag name match if present.
		private Map<String, List<Integer>> tagLineItemIndeces;

		// Map of tag filter patterns.
		private Map<Integer, Pattern> tagPatterns;

		/**
		 * List of time-ordered tag mappers for each custom tag.
		 * TagMappers are applied if active and either no tag value has yet been applied or the force flag
		 * is set for the mapper rule.
		 *
		 * List index is the tag key index.
		 */
		private List<TagMappers> tagMappers;

		private PayerAccountTagProperties(Map<String, TagConfig> tagConfigs) {
			this.tagConfigs = tagConfigs;
		}
	}

	// Map of tag configs keyed by payerAccountId.
	private Map<String, Map<String, TagConfig>> tagConfigs;

	// Map of tag properties keyed by payerAccountId.
	private Map<String, PayerAccountTagProperties> tagProperties;

	private final Map<String, Integer> tagResourceGroupIndeces;
    
    private static final String USER_TAG_PREFIX = "user:";
    private static final String AWS_TAG_PREFIX = "aws:";
    private static final String reservationIdsKeyName = "RI/SP ID";

    public BasicResourceService(ProductService productService, String[] customTags, boolean includeReservationIds) {
		super();
		this.includeReservationIds = includeReservationIds;
		this.customTags = Lists.newArrayList(customTags);
		if (includeReservationIds)
			this.customTags.add(reservationIdsKeyName);
		this.tagResourceGroupIndeces = Maps.newHashMap();
		for (int i = 0; i < customTags.length; i++)
			tagResourceGroupIndeces.put(customTags[i], i);
				
		userTagKeys = Lists.newArrayList();
		for (String tag: this.customTags) {
			if (!tag.isEmpty())
				userTagKeys.add(UserTagKey.get(tag));
		}

		this.tagProperties = Maps.newHashMap();
		this.tagConfigs = Maps.newHashMap();
	}
    
    @Override
    public Map<String, Map<String, TagConfig>> getTagConfigs() {
    	return tagConfigs;
    }
    
    @Override
    public TagMappings getTagMappings(String tagKey, String name) {
    	for (Map<String, TagConfig> maps: tagConfigs.values()) {
			TagConfig tc = maps.get(tagKey);
    		if (tc != null) {
    			for (TagMappings tm: tc.mapped) {
    				String n = tm.getName();
    				if (n != null && n.equals(name))
    					return tm;
    			}
    		}
    	}
    	return null;
    }

    @Override
    public void setTagConfigs(String payerAccountId, List<TagConfig> tagConfigs) {    	
    	if (tagConfigs == null) {
    		// Remove existing configs and indeces
    		this.tagConfigs.remove(payerAccountId);
    		tagProperties.remove(payerAccountId);
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
    	PayerAccountTagProperties properties = new PayerAccountTagProperties(configs);
    	this.tagProperties.put(payerAccountId, properties);
    	
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
		properties.tagValuesInverted = indeces;
		
		// Create the maps setting tags based on the values of other tags
		List<TagMappers> mapped = Lists.newArrayList();
		for (int tagIndex = 0; tagIndex < customTags.size(); tagIndex++) {
			String tagKey = customTags.get(tagIndex);
			TagConfig tc = configs.get(tagKey);
			if (tc == null || tc.mapped == null || tc.mapped.isEmpty()) {
				mapped.add(null);
				continue;
			}
			mapped.add(new TagMappers(tagIndex, tagKey, tc.mapped, tagResourceGroupIndeces));
		}
		properties.tagMappers = mapped;
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
		Integer index = tagResourceGroupIndeces.get(tag);
		return index == null ? -1 : index;
	}

    @Override
    public ResourceGroup getResourceGroup(Account account, Region region, Product product, LineItem lineItem, long millisStart) {
    	if (customTags.size() == 0)
    		return null;

		PayerAccountTagProperties properties = tagProperties.get(lineItem.getPayerAccountId());

        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.size()];
       	for (int i = 0; i < customTags.size(); i++) {
       		tags[i] = getUserTagValue(lineItem, customTags.get(i), properties);
       	}
       	
       	// Handle any tag mapping
    	List<TagMappers> tagMappersForPayerAccount = properties == null ? null : properties.tagMappers;
    	
       	for (int i = 0; i < customTags.size(); i++) {
       		String v = tags[i];
       		
       		// Apply tag mappers if any
       		if (tagMappersForPayerAccount != null) {
       	    	TagMappers tagMappersForKey = tagMappersForPayerAccount.get(i);
	        	if (tagMappersForKey != null)
	        		v = tagMappersForKey.getMappedUserTagValue(millisStart, account.getId(), tags, tags[i]);
       		}
       		
       		// Apply default mappings if any
        	if (v == null || v.isEmpty())
        		v = account.getDefaultUserTagValue(customTags.get(i), millisStart);
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
    
    // Used by the reservation capacity poller
    @Override
    public ResourceGroup getResourceGroup(Account account, Product product, List<com.amazonaws.services.ec2.model.Tag> reservedInstanceTags, long millisStart) {
    	if (customTags.size() == 0)
    		return null;
    	
        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.size()];
       	for (int i = 0; i < customTags.size(); i++) {
           	String v = null;
   			// find first matching key with a legitimate value
       		for (com.amazonaws.services.ec2.model.Tag riTag: reservedInstanceTags) {
       			if (riTag.getKey().toLowerCase().equals(customTags.get(i).toLowerCase())) {
       				v = riTag.getValue();
       				if (v != null && !v.isEmpty())
       					break;
       			}
       		}
        	if (v == null || v.isEmpty())
        		v = account.getDefaultUserTagValue(customTags.get(i), millisStart);
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

    protected String getUserTagValue(LineItem lineItem, String tag) {
		PayerAccountTagProperties properties = tagProperties.get(lineItem.getPayerAccountId());
		return getUserTagValue(lineItem, tag, properties);
	}

    protected String getUserTagValue(LineItem lineItem, String tag, PayerAccountTagProperties properties) {
    	if (includeReservationIds && tag == reservationIdsKeyName) {
    		String id = lineItem.getReservationArn();
    		if (id.isEmpty())
    			id = lineItem.getSavingsPlanArn();
    		return id;
    	}
    	
    	Map<String, Map<String, String>> indeces = properties.tagValuesInverted;
    	Map<String, String> invertedIndex = indeces == null ? null : indeces.get(tag);
    	
    	// Grab the first non-empty value
    	for (int index: properties.tagLineItemIndeces.get(tag)) {
    		if (lineItem.getResourceTagsSize() > index) {
    	    	// cut all white space from tag value
    			String val = stripSpaces(lineItem.getResourceTag(index));
    			
    			if (!StringUtils.isEmpty(val)) {
    				if (invertedIndex != null && invertedIndex.containsKey(val.toLowerCase())) {
						val = invertedIndex.get(val.toLowerCase());
					}
	    			return filter(val, index, tag, properties);
    			}
    		}
    	}
    	return null;
    }

    private String filter(String value, int index, String tag, PayerAccountTagProperties properties) {
		if (properties.tagConfigs != null) {
			TagConfig tc = properties.tagConfigs.get(tag);
			if (tc != null && tc.getConvert() != null) {
				switch(tc.getConvert()) {
					case toLower: value = value.toLowerCase(); break;
					case toUpper: value = value.toUpperCase(); break;
					default: break;
				}
			}
		}
    	if (properties.tagPatterns != null) {
			Pattern filter = properties.tagPatterns.get(index);
			if (filter != null) {
				Matcher m = filter.matcher(value);
				if (m.find())
					value = m.group();
				else
					value = "Other";
			}
		}
		return value;
	}

    @Override
    public boolean[] getUserTagCoverage(LineItem lineItem) {
    	boolean[] userTagCoverage = new boolean[userTagKeys.size()];
		PayerAccountTagProperties properties = tagProperties.get(lineItem.getPayerAccountId());

        for (int i = 0; i < userTagKeys.size(); i++) {
        	String v = getUserTagValue(lineItem, userTagKeys.get(i).name, properties);
        	userTagCoverage[i] = !StringUtils.isEmpty(v);
        }    	
    	return userTagCoverage;
    }

    @Override
    public void commit() {

    }
    
    @Override
    public void initHeader(String[] header, String payerAccountId) {
    	PayerAccountTagProperties properties = tagProperties.get(payerAccountId);
    	if (properties == null) {
    		// Must not have had any tag configs for the payer
			properties = new PayerAccountTagProperties(tagConfigs.get(payerAccountId));
			tagProperties.put(payerAccountId, properties);
		}
    	properties.tagLineItemIndeces = Maps.newHashMap();
    	properties.tagPatterns = Maps.newHashMap();

    	/*
    	 * Create a list of billing report line item indeces for
    	 * each of the configured user tags. The list will first have
    	 * the exact match for the name if it exists in the report
    	 * followed by any case variants and specified aliases
    	 */
    	for (UserTagKey tagKey: userTagKeys) {
    		String fullTag = USER_TAG_PREFIX + tagKey.name;
    		List<Integer> indeces = Lists.newArrayList();
    		properties.tagLineItemIndeces.put(tagKey.name, indeces);
    		
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
    		// Look for alternate names with only case variation
            for (int i = 0; i < header.length; i++) {
            	if (i == index) {
            		continue;	// skip the exact match we handled above
            	}
            	if (fullTag.equalsIgnoreCase(header[i])) {
            		indeces.add(i);
            	}
            }
            // Look for aliases
            if (properties.tagConfigs != null && properties.tagConfigs.containsKey(tagKey.name)) {
            	TagConfig config = properties.tagConfigs.get(tagKey.name);

            	if (config != null) {
					// Assign any filters to the preferred key name and case variations
					if (config.filter != null && !config.filter.isEmpty()) {
						Pattern pattern = Pattern.compile(config.filter, Pattern.CASE_INSENSITIVE);
						for (int j: indeces)
							properties.tagPatterns.put(j, pattern);
					}

					if (config.aliases != null) {
						for (TagConfig.KeyAlias alias : config.aliases) {
							String fullAliasName = alias.name.startsWith(AWS_TAG_PREFIX) ? alias.name : USER_TAG_PREFIX + alias.name;
							for (int i = 0; i < header.length; i++) {
								if (fullAliasName.equalsIgnoreCase(header[i])) {
									indeces.add(i);

									// Compile and store any filter patterns
									if (alias.filter != null && !alias.filter.isEmpty())
										properties.tagPatterns.put(i, Pattern.compile(alias.filter, Pattern.CASE_INSENSITIVE));
								}
							}
						}
					}
				}
            }
    	}
    }
}
