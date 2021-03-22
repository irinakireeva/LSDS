package upf.edu;

import upf.edu.storage.DynamoHashTagRepository;

public class TwitterHashtagsReader {
    public static void main(String[] args){
        String lang = args[0];
        DynamoHashTagRepository hashTagRepository = new DynamoHashTagRepository();

        System.out.println(hashTagRepository.readTop10(lang).toString());
    }
}
