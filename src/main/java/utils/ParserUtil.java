package utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import models.JsonModel;

/**
 * Created by jejoseph on 5/8/17.
 */
public class ParserUtil {

    public static JsonModel readJson(String originalMessage) {
        JsonModel message = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
            System.out.println("originalMessage is " + originalMessage.substring(0, 200));
            message = mapper.readValue(originalMessage, JsonModel.class);
        } catch (Exception e) {
            System.out.println("Error mapping json:" + e.getMessage());
            e.printStackTrace();
        }

        return message;
    }
}
