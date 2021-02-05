package upf.edu.filter;

import java.io.*;
import java.util.Scanner;

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
                        System.out.println("Tweet parser language: "+tweetParser.get().getLanguage());
                        System.out.println(language);
                        if(tweetParser.get().getLanguage() == language){
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
