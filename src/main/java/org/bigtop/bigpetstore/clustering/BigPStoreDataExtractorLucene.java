package org.bigtop.bigpetstore.clustering;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.TextContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.*;
import java.nio.CharBuffer;
import java.util.List;

/**
 * Created by ubu on 2/1/14.
 */
public class BigPStoreDataExtractorLucene {

    final static Logger log = LoggerFactory.getLogger(BigPStoreDataExtractorLucene.class);

    public static DataModel  doBigPStoreDataExtractor(File dataFile) throws Exception {
        DataModel model = null;
        final List<String> fields = Lists.newArrayList();
        AutoDetectParser p = new AutoDetectParser();

        FileInputStream fileStream=new FileInputStream(dataFile);

        byte[] arr= new byte[(int)dataFile.length()];

        fileStream.read(arr,0,arr.length);

        p.parse(new ByteArrayInputStream(arr), new TextContentHandler(new DefaultHandler()  {

                    @Override
                    public void characters(char[] ch, int start, int length) throws SAXException {
                        CharBuffer buf = CharBuffer.wrap(ch, start, length);
                        String s = buf.toString();
                        fields.add(s);
                    }
                }), new Metadata());

        log.info("BigPStoreDataExtractorLucene "+fields);



        String tmp =Joiner.on(",")
                .join(fields)
                .replaceAll("\\s+", " ");



        tmp =tmp.replaceAll(",", " ");

        log.info("BigPStoreDataExtractorLucene tmp "+tmp);

        StringReader in = new StringReader(tmp.replaceAll("\\s+", " "));
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
        TokenStream ts = analyzer.tokenStream("content", in);
        ts = new LowerCaseFilter(Version.LUCENE_43, ts);
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        List<String> words = Lists.newArrayList();

        while (ts.incrementToken()) {
            char[] termBuffer = termAtt.buffer();
            int termLen = termAtt.length();
            String w = new String(termBuffer, 0, termLen);
            words.add(w);
        }


        log.info("BigPStoreDataExtractorLucene words for analysis "+words);

        return model;
    }



}
