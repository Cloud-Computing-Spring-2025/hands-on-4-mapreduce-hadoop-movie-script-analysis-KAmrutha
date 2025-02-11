package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable dialogueSize = new IntWritable();
    private Text speakerName = new Text();

    @Override
    public void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
        String lineContent = inputValue.toString().trim();

        if (!lineContent.isEmpty() && lineContent.contains(":")) {
            String[] parts = lineContent.split(":", 2);
            if (parts.length < 2) return;

            speakerName.set(parts[0].trim());
            dialogueSize.set(parts[1].trim().length());

            context.write(speakerName, dialogueSize);
        }
    }
}
