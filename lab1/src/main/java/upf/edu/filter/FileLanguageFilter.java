package upf.edu.filter;

import java.io.*;

public class FileLanguageFilter {
    private final String InputFile;
    private final String OutputFile;

    public FileLanguageFilter(String inputFile, String outputFile) {
        InputFile = inputFile;
        OutputFile = outputFile;
    }

    public void filterLanguage(String language) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(this.InputFile));
        FileWriter writer = new FileWriter(this.OutputFile);
        BufferedWriter bw = new BufferedWriter(writer);

        String line = br.readLine();
        while (line != null) {
            System.out.println(line);
            // read next line
            line = br.readLine();
        }
        br.close();
        bw.close();

    }
}
