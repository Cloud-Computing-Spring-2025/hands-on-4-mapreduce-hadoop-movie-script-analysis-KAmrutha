package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable count = new IntWritable(1);
    private Text wordKey = new Text();

    @Override
    public void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
        String lineText = inputValue.toString().trim();

        if (!lineText.isEmpty() && lineText.contains(":")) {
            String[] segments = lineText.split(":", 2);
            if (segments.length < 2) return;

            String speaker = segments[0].trim();
            String speech = segments[1].trim();

            StringTokenizer tokenizer = new StringTokenizer(speech);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!token.isEmpty()) {
                    wordKey.set(speaker + "-" + token);
                    context.write(wordKey, count);
                }
            }
        }
    }
}
