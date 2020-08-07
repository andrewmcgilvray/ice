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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.AwsUtils;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.processor.ReservationService;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Product.Source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

public class BasicProductService implements ProductService {
    Logger logger = LoggerFactory.getLogger(getClass());
    
    final private String productsFileName = "products.csv";
	final private String[] header = new String[]{ "Name", "ServiceName", "ServiceCode", "Source" };

	private static Map<String, String> missingServiceNames = Maps.newHashMap();

	static {
		// Missing service names from pricing service as of Aug2019
		missingServiceNames.put("AmazonETS", "Amazon Elastic Transcoder");
		missingServiceNames.put("AWSCodeCommit", "AWS CodeCommit");
		missingServiceNames.put("AWSDeveloperSupport", "AWS Support (Developer)");
		missingServiceNames.put("AWSSupportBusiness", "AWS Support (Business)");
		missingServiceNames.put("AWSSupportEnterprise", "AWS Support (Enterprise)");
		missingServiceNames.put("datapipeline", "AWS Data Pipeline");
	}
	
	/*
	 * Product lookup maps built dynamically as we encounter the product names while
	 * processing billing reports or reading tagdb files.
	 * 
	 * Map of products keyed by the full Amazon name including the AWS and Amazon prefix
	 */
    private ConcurrentMap<String, Product> productsByServiceName = Maps.newConcurrentMap();
    /*
     * Map of products keyed by the name without the AWS or Amazon prefix. Also has entries for override names
     */
    private ConcurrentMap<String, Product> productsByName = Maps.newConcurrentMap();
    /*
     * Map of products keyed by the AWS service code used for saving the data
     */
    private ConcurrentMap<String, Product> productsByServiceCode = Maps.newConcurrentMap();
    
    /*
     * Mutex for locking on the addProduct operation. We want the same product object to be
     * used across all the separate maps.
     */
    private final Lock lock = new ReentrantLock(true);
       
    public BasicProductService() {
		super();
	}
    
    public void initReader(String localDir, String bucket, String prefix) {
    	retrieve(localDir, bucket, prefix);
    }
    
    public void initProcessor(String localDir, String bucket, String prefix) {
    	retrieve(localDir, bucket, prefix);
    	
    	// Build/Amend the product list using the AWS Pricing Service
    	Map<String, String> serviceNames = AwsUtils.getAwsServiceNames();
   	
    	for (String code: serviceNames.keySet()) {
    		String name = serviceNames.get(code);
    		// See if we already have this service in the map
    		Product existing = productsByServiceCode.get(code);
    		if (existing != null) {
    			if (name != null && !existing.getServiceName().equals(name))
    				logger.warn("Found service with different name than one used in CUR for code: " + code + ", Pricing Name: " + name + ", CUR Name: " + existing.getServiceName());
    			continue;
    		}
    		
    		if (name == null) {
    			// Not all services return a service name even though they have one.
    			// Handle the one we know about and just use the Code for those we don't.
    			if (missingServiceNames.containsKey(code))
    				name = missingServiceNames.get(code);
    			else
    				name = code;
    			
    			logger.warn("Service " + code + " doesn't have a service name, use: " + name);
    		}
    		addProduct(new Product(name, code, Source.pricing));
    	}
    	
    	// Add products that aren't included in the pricing service list (as of Aug2019)
    	if (!productsByServiceCode.containsKey("AmazonRegistrar"))
    		addProduct(new Product("Amazon Registrar", "AmazonRegistrar", Source.code));
    	if (!productsByServiceCode.containsKey("AWSSecurityHub"))
    		addProduct(new Product("AWS Security Hub", "AWSSecurityHub", Source.code));
    	
    	// Add products for the ICE breakouts
    	addProduct(new Product(Product.Code.Ec2Instance));
    	addProduct(new Product(Product.Code.RdsInstance));
    	addProduct(new Product(Product.Code.Ebs));
    	addProduct(new Product(Product.Code.Eip));
    }

	public Product getProduct(String serviceName, String serviceCode) {
		if (StringUtils.isEmpty(serviceCode))
			return getProductByServiceName(serviceName);
		
        Product product = productsByServiceCode.get(serviceCode);
        if (product == null) {
        	// Use the service code for the name if the service name is empty
            Product newProduct = new Product(StringUtils.isEmpty(serviceName) ? serviceCode : serviceName, serviceCode, Source.cur);
            product = addProduct(newProduct);
            if (newProduct == product)
            	logger.info("created product: " + product.getIceName() + " for: " + serviceName + " with code: " + product.getServiceCode());
            else if (!product.getServiceCode().equals(serviceCode)) {
            	logger.error("new service code " + serviceCode + " for product: " + product.getIceName() + " for: " + serviceName + " with code: " + product.getServiceCode());
            }
        }
        else if (!serviceName.isEmpty() && !product.getServiceName().equals(serviceName) && 
        		(product.getSource() == Source.pricing ||
        			(product.getSource() == Source.cur && product.getServiceName().equals(product.getServiceCode()))
        		)) {
        	// Service name doesn't match, update the product with the proper service name
        	// assuming that billing reports always have more accurate names than the pricing service
        	// or the previous value from the billing report was empty and set to match the serviceCode
        	product = addProduct(new Product(serviceName, product.getServiceCode(), Source.cur));
        }
        return product;
    }
	
    public Product getProduct(Product.Code code) {
    	Product product = productsByServiceCode.get(code.serviceCode);
    	if (product == null) {
    		Product newProduct = new Product(code);
    		product = addProduct(newProduct);
            if (newProduct == product)
            	logger.info("created product: " + product.getIceName() + " for: " + code.serviceName + " with code: " + product.getServiceCode());
            else if (!product.getServiceCode().equals(code.serviceCode)) {
            	logger.error("new service code " + code.serviceCode + " for product: " + product.getIceName() + " for: " + code.serviceName + " with code: " + product.getServiceCode());
            }
    	}
    	return product;
    }

    
	/*
	 * Called by BasicManagers to manage product list for resource-based cost and usage data files.
	 */
    public Product getProductByServiceCode(String serviceCode) {
    	// Look up the product by the AWS service code
    	Product product = productsByServiceCode.get(serviceCode);
    	if (product == null) {
    		product = new Product(serviceCode, serviceCode, Source.code);
    		product = addProduct(product);
            logger.warn("created product by service code: " + serviceCode + ", name: "+ product.getIceName() + ", code: " + product.getServiceCode());
    	}
    	return product;
    }

    public Product getProductByServiceName(String serviceName) {
        Product product = productsByServiceName.get(serviceName);
        if (product == null) {
            product = new Product(serviceName, serviceName.replace(" ", ""), Source.dbr);
            product = addProduct(product);
            logger.info("created product by service name: \"" + product.getServiceName() + "\" for code: " + product.getServiceCode() + ", iceName: \"" + product.getIceName() + "\"");
        }
        return product;
    }
        
    protected Product addProduct(Product product) {
    	lock.lock();
    	try {    	
	    	// Check again now that we hold the lock
    		Product existingProduct = productsByServiceCode.get(product.getServiceCode());
	    	if (existingProduct != null) {
	            if (product.getServiceName().equals(existingProduct.getServiceName()))
	            	return existingProduct;
	            
	            logger.warn("service name does not match for " + product.getServiceCode() + ", existing: " + existingProduct.getServiceName() + ", replace with: " + product.getServiceName());
	    	}
			
	    	setProduct(product);
	        return product;
    	}
    	finally {
    		lock.unlock();
    	}
    }
    
    private void setProduct(Product product) {
        productsByName.put(product.getIceName(), product);
        productsByServiceCode.put(product.getServiceCode(), product);

        String canonicalName = product.getCanonicalName();
        if (!canonicalName.equals(product.getIceName())) {
        	// Product is using an alternate name, also save the canonical name
        	productsByName.put(canonicalName, product);
        }
        
        productsByServiceName.put(product.getServiceName(), product);
    }

    public Collection<Product> getProducts() {
        return productsByServiceCode.values();
    }

    public List<Product> getProducts(List<String> names) {
    	List<Product> result = Lists.newArrayList();
    	for (String name: names) {
    		Product p = productsByName.get(name);
    		if (p == null)
    			logger.error("Unable to find product by name: " + name);
    		else
    	    	result.add(p);
    	}
    	return result;
    }

    public void archive(String localDir, String bucket, String prefix) throws IOException {
        
        File file = new File(localDir, productsFileName);
        
    	OutputStream os = new FileOutputStream(file);
		Writer out = new OutputStreamWriter(os);
        
        try {
        	writeCsv(out);
        }
        finally {
            out.close();
        }

        // archive to s3
        logger.info("uploading " + file + "...");
        AwsUtils.upload(bucket, prefix, localDir, file.getName());
        logger.info("uploaded " + file);
    }

    protected void writeCsv(Writer out) throws IOException {
    	
    	CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(header));
    	for (Product p: productsByServiceCode.values()) {
    		printer.printRecord(p.getIceName(), p.getServiceName(), p.getServiceCode(), p.getSource());
    	}
  	
    	printer.close(true);
    }
    
    public void updateReader(String localDir, String bucket, String prefix) {
        File file = new File(localDir, productsFileName);
    	
        boolean downloaded = AwsUtils.downloadFileIfChanged(bucket, prefix, file);
        if (downloaded) {
        	logger.info("downloaded " + file);
        	load(file);
        }        
    }
    
    private void retrieve(String localDir, String bucket, String prefix) {
        File file = new File(localDir, productsFileName);
    	
        boolean downloaded = false;
        try {
        	downloaded = AwsUtils.downloadFileIfChanged(bucket, prefix, file);
        }
        catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404)
                throw e;
            logger.info("file not found in s3 " + file);
        }
        if (downloaded)
        	logger.info("downloaded " + file);
        
        if (file.exists()) {
        	load(file);
        }        
    }
    
    private void load(File file) {
        BufferedReader reader = null;
        try {
        	InputStream is = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(is));
            readCsv(reader);
        }
        catch (Exception e) {
        	Logger logger = LoggerFactory.getLogger(ReservationService.class);
        	logger.error("error in reading " + file, e);
        }
        finally {
            if (reader != null)
                try {reader.close();} catch (Exception e) {}
        }
    }
    
    protected void readCsv(Reader reader) throws IOException {
    	Iterable<CSVRecord> records = CSVFormat.DEFAULT
    		      .withHeader(header)
    		      .withFirstRecordAsHeader()
    		      .parse(reader);
    	
	    for (CSVRecord record : records) {
	    	String iceName = record.get(0);
	    	String serviceName = record.get(1);
	    	String serviceCode = record.get(2);
	    	Source source = Source.valueOf(record.get(3));
	    	
    		Product existingProduct = productsByServiceCode.get(serviceCode);
	    	if (existingProduct != null) {
	    		existingProduct.update(serviceName, iceName);
		    }
	    	else {	    	
	    		setProduct(new Product(serviceName, serviceCode, source));	
	    	}
	    }
    }
}
