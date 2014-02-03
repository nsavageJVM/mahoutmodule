package org.bigtop.bigpetstore.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.mahout.cf.taste.model.DataModel;
import org.bigtop.bigpetstore.clustering.BigPStoreDataExtractorLucene;
import org.bigtop.bigpetstore.clustering.PersonInputSplit;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ubu on 2/1/14.
 */
public class MahoutMathTest  {

    final static Logger log= LoggerFactory.getLogger(MahoutMathTest.class);

    Path output = new Path("petstoredata/baseData");

    @Test
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


    @Test
    public void testEnums() throws Exception {

        Pair<String,Integer>  results = combineEnumsForPatterns(5, TransactionIteratorFactory.STATE.AK);


    }


    // 50% chance product will relate to a person
    private Pair<String,Integer> combineEnumsForPatterns(int personClassifier, final TransactionIteratorFactory.STATE state) {

        Pair<String,Integer> product_price = null;
        List<TransactionIteratorFactory.Person> personProductList =
                Arrays.asList(TransactionIteratorFactory.Person.values());
        // create a filter to create person types

        switch (personClassifier) {
            case 1:  case 2:  product_price = personProductList.get(0).createPersonProduct();
                System.out.println(" create product on dog person.createPersonProduct(0) "+personClassifier);
                System.out.println(" create product on person.createPersonProduct(0) "+product_price.getFirst());
                break;
            case 5:  case 6:  product_price =  personProductList.get(1).createPersonProduct();
                System.out.println(" create product on cat person.createPersonProduct(1) "+personClassifier);
                System.out.println(" create product on person.createPersonProduct(1) "+product_price.getFirst());
                break;
            case 9:  case 10:  product_price =  personProductList.get(2).createPersonProduct();
                System.out.println(" create product on fish person.createPersonProduct(2) "+personClassifier);
                System.out.println(" create product on person.createPersonProduct(2) "+product_price.getFirst());
                break;
            case 15:  case 16:  product_price =  personProductList.get(3).createPersonProduct();
                System.out.println(" create product on bird person.createPersonProduct(3) "+personClassifier);
                System.out.println(" create product on person.createPersonProduct(3) "+product_price.getFirst());
                break;
            case 20:  case 21: case 22: case 27: product_price =  personProductList.get(4).createPersonProduct();
                System.out.println(" create product on hamster person.createPersonProduct(4) "+personClassifier);
                System.out.println(" create product on person.createPersonProduct(4) "+product_price.getFirst());
                break;
            default:   product_price = state.randProduct();
                System.out.println(" create product on non person type ");
                System.out.println(" create product on person.createPersonProduct(4) "+ product_price.getFirst() );
                break;

        }


        return product_price;
    }


}
