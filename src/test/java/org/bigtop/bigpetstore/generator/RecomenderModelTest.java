package org.bigtop.bigpetstore.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.knn.ConjugateGradientOptimizer;
import org.apache.mahout.cf.taste.impl.recommender.knn.KnnItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.knn.Optimizer;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

/**
 * Created by ubu on 2/22/14.
 */
public class RecomenderModelTest {

    final static Logger log= LoggerFactory.getLogger(RecomenderModelTest.class);
    Path output = new Path("petstoredata");

    String userIdForTest = null;
    @Before
    public void setUpTestData() throws Exception {

        File file = new File(output.toString()+"/recommend.csv");
        try {
            Scanner scan = new Scanner(file);

            String line = scan.nextLine();
            String[] tokens = line.split("\t");
            userIdForTest = tokens[0];

            log.info(line);



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void phaseOneModel() throws Exception {
        log.info(output.toString()+"/recommend.csv");
        DataModel model =  new FileDataModel(new File(output.toString()+"/recommend.csv"));



        Recommender recommender = new SlopeOneRecommender(model);

        Recommender cachingRecommender = new CachingRecommender(recommender);
        //recommend(user, num recomends)
        List<RecommendedItem> recommendations = cachingRecommender.recommend(Integer.valueOf(userIdForTest), 10);

        log.info("recommendations "+recommendations.size());
        for (RecommendedItem recommendation : recommendations) {
            log.info(recommendation.toString());
        }

    }

   // @Test
    public void phaseTwoModel() throws Exception {

        RandomUtils.useTestSeed();

        DataModel model =  new FileDataModel(new File(output.toString()+"/recommend.csv"));

        RecommenderEvaluator evaluator =
                new AverageAbsoluteDifferenceRecommenderEvaluator();

        RecommenderBuilder builder = new RecommenderBuilder() {

            public Recommender buildRecommender(DataModel model)
                    throws TasteException {
                        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
                        UserNeighborhood neighborhood = new NearestNUserNeighborhood(1, similarity, model);
                return new GenericUserBasedRecommender(model, neighborhood, similarity);

            }
        };
        double score = evaluator.evaluate(  builder, null, model, 0.7, 1.0);
        log.info("score " + score);


    }
}
