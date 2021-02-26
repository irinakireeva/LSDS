package upf.edu.filter;

import java.io.*;
import java.util.Optional;
import java.util.Scanner;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import upf.edu.parser.SimplifiedTweet;

public class FileLanguageFilter {
    private final String InputFile;
    private final String OutputFile;

    public FileLanguageFilter(String inputFile, String outputFile) {
        InputFile = inputFile;
        OutputFile = outputFile;
    }

    public void filterLanguage(String language) throws IOException {
        Optional<SimplifiedTweet> tweetParser;
        String line;
        BufferedReader br = new BufferedReader(new FileReader(this.InputFile));
        FileWriter writer = new FileWriter(this.OutputFile, true);
        BufferedWriter bw = new BufferedWriter(writer);

        String tweet = br.readLine();
        while (tweet != null) {
				// if line is empty or too short then skip line
                if (tweet.length() < 2) {
                    tweet = br.readLine();
                    continue;
                } else{
                    //Parse the line
                    tweetParser = SimplifiedTweet.fromJson(tweet);
                    if (tweetParser.isPresent()){
                        if(tweetParser.get().getLanguage().equals(language)){
                            line = tweetParser.get().toString();
                            bw.write(line);
                            bw.newLine();
                        }
                    }
                }
                tweet = br.readLine();
                
                
                
            }
        br.close();
        bw.close();

    }
}
