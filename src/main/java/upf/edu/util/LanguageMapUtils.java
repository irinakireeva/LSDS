package upf.edu.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {

        JavaPairRDD<String, String> languageMap = lines
                .mapToPair(lang -> new Tuple2<>(lang.split("\t")[1],lang.split("\t")[2]));

        languageMap = languageMap
                .filter(lang -> !lang._1().equals(""));

        return languageMap;
    }
}
