package upf.edu.filter;

import upf.edu.parser.SimplifiedTweet;

import java.io.*;
import java.util.Optional;

public class FileLanguageFilter {
    private final String InputFile;
    private final String OutputFile;

    public FileLanguageFilter(String inputFile, String outputFile) {
        InputFile = inputFile;
        OutputFile = outputFile;
    }

    public void filterLanguage(String language) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(this.InputFile));
        FileWriter writer = new FileWriter(this.OutputFile, true);
        BufferedWriter bw = new BufferedWriter(writer);
        String line;
        Optional<SimplifiedTweet> tweetParser = null;

        String tweet = br.readLine();
        while (tweet != null) {
            if (tweetParser.isPresent()){
                line = tweetParser.toString();
                bw.write(line);
                bw.newLine();
            }
        }
        br.close();
        bw.close();

    }
}
