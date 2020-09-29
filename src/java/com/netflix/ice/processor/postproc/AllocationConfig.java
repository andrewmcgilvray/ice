package com.netflix.ice.processor.postproc;

import java.util.Map;

import com.netflix.ice.common.TagMappings;
import com.netflix.ice.processor.config.S3BucketConfig;
import com.netflix.ice.processor.config.KubernetesConfig;

public class AllocationConfig {
	private S3BucketConfig s3Bucket;
	private KubernetesConfig kubernetes;
	// Input tag map = UserTag Key is the map key, Report Column name is the map value.
	private Map<String, String> in;
	// Output tag map = UserTag Key is the map key, Report Column name is the map value.
	private Map<String, String> out;
	private Map<String, TagMappings> tagMaps;
	
	public S3BucketConfig getS3Bucket() {
		return s3Bucket;
	}
	public void setS3Bucket(S3BucketConfig s3Bucket) {
		this.s3Bucket = s3Bucket;
	}
	public KubernetesConfig getKubernetes() {
		return kubernetes;
	}
	public void setKubernetes(KubernetesConfig kubernetes) {
		this.kubernetes = kubernetes;
	}
	public Map<String, String> getIn() {
		return in;
	}
	public void setIn(Map<String, String> in) {
		this.in = in;
	}
	public Map<String, String> getOut() {
		return out;
	}
	public void setOut(Map<String, String> out) {
		this.out = out;
	}	
	public Map<String, TagMappings> getTagMaps() {
		return tagMaps;
	}
	public void setTagMaps(Map<String, TagMappings> tagMaps) {
		this.tagMaps = tagMaps;
	}
}
