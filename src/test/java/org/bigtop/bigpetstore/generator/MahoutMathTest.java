package org.bigtop.bigpetstore.generator;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVWriter;
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

   //
    public void test() throws Exception {

        Map<String, Map<String, Integer>> productTable = Maps.newHashMap();
        List<String> states  = ImmutableList.of("AZ", "AK", "CT", "OK", "CO", "CA", "NY");

        Map<String, Integer> rowMapAZ = new ImmutableMap.Builder<String, Integer>()
                .put("dog-food_10", 1)
                .put("cat-food_8", 2)
                .put("leather-collar_25", 3)
                .put("snake-bite ointment_30", 4)
                .put("turtle-food_11", 5)
                .build();

        productTable.put(states.get(0), rowMapAZ);

        Map<String, Integer> rowMapAK = new ImmutableMap.Builder<String, Integer>()
                .put("dog-food_10", 1)
                .put("cat-food_8", 2)
                .put("fuzzy-collar_19", 3)
                .put("antelope-caller_20", 4)
                .put("salmon-bait_30", 5)
                .build();

        productTable.put(states.get(1), rowMapAK);

        Map<String, Integer> rowMapCT = new ImmutableMap.Builder<String, Integer>()
                .put("dog-food_10", 1)
                .put("cat-food_8", 2)
                .put("fuzzy-collar_19", 3)
                .put("turtle-pellets_5", 4)
                .build();

        productTable.put(states.get(2), rowMapCT);

        Map<String, Integer> rowMapOK = new ImmutableMap.Builder<String, Integer>()
                .put("dog-food_10", 1)
                .put("cat-food_8", 2)
                .put("duck-caller_13", 3)
                .put("rodent-cage_40", 4)
                .put("hay-bail_5", 5)
                .put("cow-dung_2", 6)
                .build();

        productTable.put(states.get(3), rowMapOK);

        Map<String, Integer> rowMapCO = new ImmutableMap.Builder<String, Integer>()
                .put("dog-food_10", 1)
                .put("cat-food_8", 2)
                .put("choke-collar_15", 3)
                .put("antelope snacks_30", 4)
                .put("duck-caller_18", 5)
                .build();

        productTable.put(states.get(4), rowMapCO);

        Map<String, Integer> rowMapCA = new ImmutableMap.Builder<String, Integer>()
                .put("dog-food_10", 1)
                .put("cat-food_8", 2)
                .put("fish-food_12", 3)
                .put("organic-dog-food_16", 4)
                .put("turtle-pellets_5", 5)
                .build();

        productTable.put(states.get(5), rowMapCA);

        Map<String, Integer> rowMapNY = new ImmutableMap.Builder<String, Integer>()
                .put("dog-food_10", 1)
                .put("cat-food_8", 2)
                .put("steel-leash_20", 3)
                .put("fish-food_20", 4)
                .put("seal-spray_25", 5)
                .build();

        productTable.put(states.get(6),rowMapNY );


            System.out.println(productTable);


        String csvLine = "BigPetStore,storeCode_AK,1\tchris,whitaker,Wed Dec 17 03:38:36 EET 1969,15.1,choke-collar";


        String[] lines = new CSVParser().parseLine(csvLine);
        String[] tmp = lines[1].split("_");
        String state = tmp[1];
        String product = lines[lines.length -1];
        System.out.println("state "+state+" product "+product+" preference ");

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

    @Test
    public void setUpPreferenceData() throws Exception {
        // this is a filter set where the filter is a list of products with their probability
        EnumSet<PreferenceValues> filterSet = EnumSet.allOf(PreferenceValues.class);

        // get the raw data to filter
        FileReader fileReader = new FileReader(new File("./petstoredata/baseData/part-r-00000"));
        // write results
        CSVWriter writer = new CSVWriter(new FileWriter("./petstoredata/recommend.csv"), ',',CSVWriter.NO_QUOTE_CHARACTER);

        List<String[]> dataOutBuffer = Lists.newArrayList();
        BufferedReader br = new BufferedReader(fileReader);
        String csvLine = null;
        // if no more lines the readLine() returns null
        while ((csvLine = br.readLine()) != null) {
            Map<String, Integer> result = null;
            String[] lines = new CSVParser().parseLine(csvLine);
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
                    String[] entries = {nameId.toString(), result.get("filterId").toString(),  result.get("filterPrefValue").toString() };
                    dataOutBuffer.add(entries);
                    }
                }
            }

        }
        writer.writeAll(dataOutBuffer);
        writer.close();
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
    }
