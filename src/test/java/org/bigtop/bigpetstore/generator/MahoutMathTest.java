package org.bigtop.bigpetstore.generator;

import com.google.common.collect.*;
import org.apache.commons.lang3.StringUtils;
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
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by ubu on 2/1/14.
 */
public class MahoutMathTest  {

    final static Logger log= LoggerFactory.getLogger(MahoutMathTest.class);

    Path output = new Path("petstoredata/baseData");

    @Before
    public void setUpTestData() throws Exception {
        int records = 100;
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
    public void setUpPreferenceData() throws Exception {
        // this is a filter set where the filter is a list of products with their probability
        EnumSet<PreferenceValues> filterSet = EnumSet.allOf(PreferenceValues.class);

        // get the raw data to filter
        FileReader fileReader = new FileReader(new File("./petstoredata/baseData/part-r-00000"));

        List<String[]> dataOutBuffer = Lists.newArrayList();
        BufferedReader br = new BufferedReader(fileReader);
        String csvLine = null;
        // if no more lines the readLine() returns null
        while ((csvLine = br.readLine()) != null) {
            Map<String, Integer> result = null;
            String[] lines = csvLine.split(",");
            String[] tmp = lines[1].split("_");
            String state = tmp[1];
            //last 1
            String product = lines[lines.length -1];
            // new code
            String[] tmp1 = lines[2].split("\t");
            String firstName = tmp1[1];
            String lastName = lines[3];
            String hashName = firstName+lastName;
            Integer nameId = Math.abs(hashName.hashCode());
            for(PreferenceValues filter : filterSet){

                if(filter.getPref(product, filter.name()) != null  ) {
                    result =filter.getPref(product, filter.name());

                    if (result.get("match").intValue() == 1) {
                        // match result data
                        String data = "found pref value: "+result.get("filterPrefValue")+" for filter name "+
                                filter.name()+ " with user hash value "+nameId+ " and productId "+result.get("filterId");
                        System.out.println(data);
                        String[] entries = {nameId.toString(), result.get("filterId").toString(),
                                result.get("filterPrefValue").toString(), ""+new Date().getTime()  };
                        dataOutBuffer.add(entries);
                    }
                }
            }

        }

        File file = new File("./petstoredata/recommend.csv");
        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        for ( String[] sList: dataOutBuffer) {
           String csvOutLine = sList[0]+ '\t' +sList[1]+ '\t'  +sList[2]+ '\t' +sList[3]+ '\n';
            bw.write(csvOutLine);
        }
        bw.flush();
        fw.close();
    }



    public static enum PreferenceValues {

        No_Pref("0", "dog-food_10","choke-collar_15", "leather-collar_25","duck-caller_13"),
        Low_Pref("1", "cat-food_8","fuzzy-collar_19","salmon-bait_30", "antelope-caller_20"),
        Mid_Pref("2", "fish-food_20","turtle-pellets_5","seal-spray_25","salmon-bait_30", "snake-bite ointment_30"),
        Med_Pref( "3", "choke-collar_15", "antelope snacks_30", "hay-bail_5","cow-dung_2", "turtle-food_11"),
        High_Pref("4", "rodent-cage_40","antelope snacks_30","hay-bail_5","steel-leash_20","organic-dog-food_16" );

        Integer filterId = null;
        public String[] preferences;
        // constructor
        private PreferenceValues( String... preferences) {

            this.preferences = preferences;
        }

        public Map<String , Integer> getPref(String match, String filterName) {
            Integer pref = null;
            Pair<String,Integer> pair = null;
            Map<String, Integer> result = Maps.newHashMap();
            for(String s :preferences) {
                String[] tmp  = s.split("_");
                if (match.equals(tmp[0])) {
                    pref = Integer.parseInt(preferences[0]);
                    filterId = Math.abs(s.hashCode());
                    System.out.println("matched element "+match+" for filter "+filterName+", with preference = "+pref);
                    result.put("filterId", filterId);
                    result.put("filterPrefValue", pref);
                    result.put("match", 1);
                    break;
                } else {
                    result.put("match", Integer.valueOf(-1));

                }


            }
            return result;
        }
    }






  private void doNothing() throws Exception {
      List<String>  model =
              BigPStoreDataExtractorLucene.doBigPStoreDataExtractorRawData(
                      new File("./petstoredata/baseData/part-r-00000"));


      for (String s : model) {
          System.out.println("model.size " +model.size());
          //   System.out.println("user " +model.get(2)+ " " +model.get(3)+"product " +model.get(4));
      }

    }


    }
