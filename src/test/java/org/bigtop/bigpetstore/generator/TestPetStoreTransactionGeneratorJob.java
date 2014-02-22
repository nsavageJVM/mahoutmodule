package org.bigtop.bigpetstore.generator;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.Date;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.bigtop.bigpetstore.generator.TransactionIteratorFactory.STATE;
import org.bigtop.bigpetstore.generator.PetStoreJob.props;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * run this test with vm options -XX:MaxPermSize=256m -Xms512m -Xmx1024m
 *
 */
public class TestPetStoreTransactionGeneratorJob {

    final static Logger log= LoggerFactory.getLogger(TestPetStoreTransactionGeneratorJob.class);

    @Test
    public void test() throws Exception{

        int records = 2000;
        /**
         * Setup configuration with prop.
         */
        Configuration c=new Configuration();
        c.setInt(props.bigpetstore_records.name(), records);

        /**
         * Run the job
         */
        Path output = new Path("petstoredata");
        Job createInput=PetStoreJob.createJob(output, c);
        createInput.waitForCompletion(true);

        FileSystem fs =  FileSystem.getLocal(new Configuration());

        /**
         * Read file output into string.
         */
        DataInputStream f= fs.open(new Path(output,"part-r-00000"));
        BufferedReader br=new BufferedReader(new InputStreamReader(f));
        String s;
        int recordsSeen=0;
        boolean CTseen=false;
        boolean AZseen=false;
        
        //confirm that both CT and AZ are seen in the outputs.
        while(br.ready()){
            s=br.readLine();
            System.out.println("===>"+s);
            recordsSeen++;
            if(s.contains(STATE.CT.name())){
                CTseen=true;
            }
            if(s.contains(STATE.AZ.name())){
                AZseen=true;
            }
        }

        log.info("Created " + records + " , file was " + fs.getFileStatus(new Path(output,"part-r-00000")).getLen() + " bytes." );
    }
}
