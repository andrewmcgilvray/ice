package com.netflix.ice.reader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Zone.BadZone;

public class ReadOnlyDataTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String dataDir = "src/test/data/";
    
    private static AccountService as;
    private static ProductService ps;
    
	@BeforeClass
	public static void init() throws IOException {
		as = new BasicAccountService();
        ps = new BasicProductService();
	}
	

	@Test
	public void testFileRead() throws IOException, BadZone {
        String filename = "cost_daily_505vubukj9ayygz7z5jbws97j_2020";
        boolean forReservations = false;
        int numUserTags = 16;
       
        File file = new File(dataDir, filename + ".gz");
        
        if (!file.exists()) {
        	// Don't run the test if the file doesn't exist
        	return;
        }
    	InputStream is = new FileInputStream(file);
    	is = new GZIPInputStream(is);
        DataInputStream in = new DataInputStream(is);
        ReadOnlyData data = new ReadOnlyData(numUserTags);
        
        try {
            data.deserialize(as, ps, in, forReservations);
        }
        finally {
            if (in != null)
                in.close();
        }

        dump(data);
        
        String outFilename = dataDir + "/" + filename + "_ro.csv";
        
        FileWriter out;
		out = new FileWriter(outFilename);
        // Output CSV file
		serialize(out, data);
    	out.close();
	}
	
    private void serialize(OutputStreamWriter out, ReadOnlyData data) throws IOException {
    	out.write("num,data,account,region,zone,product,operation,usageType,usageUnits,resource\n");

        for (Integer i = 0; i < data.getNum(); i++) {
            double[] values = data.getData(i);
            if (values == null)
            	continue;
        	for (int j = 0; j < values.length; j++) {
	            TagGroup tg = data.tagGroups.get(j);
	            
	            double v = values[j];
	            if (v == 0.0)
	            	continue;
	            
	            out.write(i.toString() + ",");
	            out.write(Double.toString(v) + ",");
	            TagGroup.Serializer.serializeCsv(out, tg);
	            out.write("\n");
        	}
        }
    }
    
    private void dump(ReadOnlyData data) {
    	for (int i = 0; i < data.getNum(); i++) {
    		double[] ds = data.getData(i);
    		if (ds != null) {
    			Double[] doubleArray = ArrayUtils.toObject(ds);
	    		List<Double> d = Arrays.asList(doubleArray);
	    		logger.info("  " + i + ": " + d);    		
    		}
    	}
    }
    
}
