package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text characterKey = new Text();
    private Text uniqueWord = new Text();

    @Override
    public void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
        String textLine = inputValue.toString().trim();

        if (!textLine.isEmpty() && textLine.contains(":")) {
            String[] parts = textLine.split(":", 2);
            if (parts.length < 2) return;

            characterKey.set(parts[0].trim());

            StringTokenizer tokenizer = new StringTokenizer(parts[1]);
            HashSet<String> uniqueSet = new HashSet<>();

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!token.isEmpty()) {
                    uniqueSet.add(token);
                }
            }

            for (String word : uniqueSet) {
                uniqueWord.set(word);
                context.write(characterKey, uniqueWord);
            }
        }
    }
}
