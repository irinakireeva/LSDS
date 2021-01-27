package upf.edu.filter;

import java.io.*;

public class FileLanguageFilter implements LanguageFilter {
    public FileLanguageFilter(String inputFile, String outputFile) throws IOException {
        FileReader reader = new FileReader(inputFile);
        BufferedReader bReader = new BufferedReader(reader);

        String line = bReader.readLine();

        FileWriter writer = new FileWriter(outputFile);
        BufferedWriter bWriter = new BufferedWriter(writer);

        bWriter.write(line);

        bReader.close();
        bWriter.close();
    }

    public void filterLanguage(String language) throws Exception {

    }
}
