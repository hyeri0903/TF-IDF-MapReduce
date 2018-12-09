package kr.ac.kookmin.cs.bigdata;

import java.util.StringTokenizer;

import java.io.IOException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.MapWritable;



public class tfidf2 {
        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Create a new job
        Job job = Job.getInstance(conf, "tfidf2");

        // Use the WordCount.class file to point to the job jar
        job.setJarByClass(tfidf2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Setting the input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for it's completion
        job.waitForCompletion(true);
    }

    public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
	//private TermFrequencyWritable docFrequency = new TermFrequencyWritable();
        private final static IntWritable one = new IntWritable(1);
        private Text morph = new Text();

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
	   
            String line = value.toString();
            String[] fields = line.split("\\|");
	    String userId = fields[0]; // firstfield is user id
            String reviewField = fields[1];   //this field is review field

	    StringTokenizer iterator = new StringTokenizer(reviewField);

	    while(iterator.hasMoreTokens()){
		    morph.set(iterator.nextToken());
		    context.write(morph, new Text(userId));
	    }

           // String[] terms = midField.split(" "); //review tokenizde by " "

           // for (String t : tokens) {
           //     word.set(t);
            //    context.write(word, new Text(userId));
           // }
        }
    }

    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
	//MapWritable mw = new MapWritable();
	Map<String, Integer> tokens = null;
	Text vword = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
		tokens = new HashMap<String, Integer>();
		int totalDocs= 1411520;
		for(Text value : values)
		{
			if(tokens.containsKey(value.toString()))
			{
				int count = tokens.get(value.toString());
				count++;
				tokens.put(value.toString(), count);
			}
			else
			{
				tokens.put(value.toString(), 1);
			}
		}
		int docAppears = tokens.size(); 
		double idf = Math.log10((double) totalDocs / docAppears);
		idf = Math.round(idf*100)/100.0;		
		Set<String> keys = tokens.keySet();
		String emitValue = "";
		for(String docId : keys)
		{
			emitValue += docId + ":" + tokens.get(docId)*idf + ",";
		}
	//	emitValue = emitValue + "\tIDF Score " + idf;
		vword.set(emitValue);
		context.write(key, vword);        
		 
        }
    }
}

