package org.bigtop.bigpetstore.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.cf.taste.model.DataModel;
import org.bigtop.bigpetstore.clustering.BigPStoreDataExtractorLucene;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.InputStreamReader;

/**
 * Created by ubu on 2/1/14.
 */
public class MahoutMathTest  {

    final static Logger log= LoggerFactory.getLogger(MahoutMathTest.class);

    Path output = new Path("petstoredata/baseData");

    @Before
    public void setUpTestData() throws Exception {
        int records = 20;
        /**
         * Setup configuration with prop.
         */
        Configuration c=new Configuration();
        c.setInt(PetStoreJob.props.bigpetstore_records.name(), records);

        /**
         * Run the job
         */
        Path output = new Path("petstoredata/baseData");
        Job createInput=PetStoreJob.createJob(output, c);
        createInput.waitForCompletion(true);

        FileSystem fs =  FileSystem.getLocal(new Configuration());



    }

    @Test
    public void test() throws Exception{

        log.info("MahoutMathTest");
        FileSystem fs =  FileSystem.getLocal(new Configuration());
        Path output = new Path("petstoredata");
        DataInputStream f= fs.open(new Path(output,"part-r-00000"));
        BufferedReader br=new BufferedReader(new InputStreamReader(f));
        String s;

        while(br.ready()){
            s=br.readLine();
            log.info(s);

        }

        DataModel model = BigPStoreDataExtractorLucene.doBigPStoreDataExtractor(new File("./petstoredata/part-r-00000"));


    }


}
