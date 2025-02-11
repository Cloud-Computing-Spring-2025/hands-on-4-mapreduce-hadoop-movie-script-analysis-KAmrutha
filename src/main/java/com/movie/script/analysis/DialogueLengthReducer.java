package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalDialogueSize = 0;
        for (IntWritable count : values) {
            totalDialogueSize += count.get();
        }
        context.write(key, new IntWritable(totalDialogueSize));
    }
}
