import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class TutorialHelper {


    static void configureTwitterCredentials(String apiKey, String apiSecret, String accessToken, String accessTokenSecret) throws Exception {
        HashMap<String, String> configs = new HashMap<String, String>();
        configs.put("apiKey", apiKey);
        configs.put("apiSecret", apiSecret);
        configs.put("accessToken", accessToken);
        configs.put("accessTokenSecret", accessTokenSecret);

        Object[] keys = configs.keySet().toArray();
        for (int k = 0; k < keys.length; k++) {
            String key = keys[k].toString();
            String value = configs.get(key).trim();
            if (value.isEmpty()) {
                throw new Exception("Error setting authentication - value for " + key + " not set");
            }
            String fullKey = "twitter4j.oauth." + key.replace("api", "consumer");
            System.setProperty(fullKey, value);
            System.out.println("\tProperty " + key + " set as [" + value + "]");
        }
        System.out.println();
    }

    /** Returns the Spark URL */
    static String getSparkUrl() throws Exception {
        File file = new File("/root/spark-ec2/cluster-url");
        if (file.exists()) {
            List<String> lines = readLines(file);
            return lines.get(0);
        } else if (new File("../local").exists()) {
            return "local[4]";
        } else {
            throw new Exception("Could not find " + file);
        }
    }

    private static List<String> readLines(File file) throws IOException {
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        List<String> lines = new ArrayList<String>();
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.length() > 0) lines.add(line);
        }
        bufferedReader.close();
        return lines;
    }
}