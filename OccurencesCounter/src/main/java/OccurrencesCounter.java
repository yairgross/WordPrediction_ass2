import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OccurrencesCounter {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntArrayWritable> {
        Long numOfLines;

        public MapperClass(Long numOfLines) {
            super();
            this.numOfLines = numOfLines;
        }

        /**
         * for each ngram writes: <ngram, [partNum, OccurrencesNum]>
         * @param key - line number
         * @param value - details of ngram
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] lineDetails = itr.nextToken().split("\t");
                Text ngram = new Text(lineDetails[0]);
                IntWritable occurrences = new IntWritable(Integer.parseInt(lineDetails[2]));
                IntWritable part;
                if (key.get() < numOfLines / 2) {
                    part = new IntWritable(0);
                }
                else{
                    part = new IntWritable(1);
                }
                IntWritable[] arr = {part, occurrences};
                IntArrayWritable arrayWritable = new IntArrayWritable(arr);
//                List<IntWritable> lst = new ArrayList<IntWritable>();
//                lst.add(part);
//                lst.add(occurrences);
//                context.write(ngram, new ListOfIntWritable(lst));
                context.write(ngram, arrayWritable);
            }
        }
    }

        public static class ReducerClass extends Reducer<Text,IntArrayWritable,Text,IntArrayWritable> {
            int r0 = 0;
            int r1 = 0;
            Text currGram = null;


            /**
             * for each ngran, aggregates the number of occurrences in both parts separately, and
             * writes: <ngram, [occurrencesInPart1, occurrencesInPart2, occurrencesOverall]>
             * @param key - ngram
             * @param values - [partNum, OccurrencesNum]
             */
            @Override
            protected void reduce(Text key, Iterable<IntArrayWritable> values, Reducer<Text, IntArrayWritable, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException {
                if(key.compareTo(currGram) != 0){
//                    List<IntWritable> lst = new ArrayList<IntWritable>();
//                    lst.add(new IntWritable(r0));
//                    lst.add(new IntWritable(r1));
//                    lst.add(new IntWritable(r0 + r1));
//                    context.write(currGram, new ListOfIntWritable(lst));
                    IntWritable[] arr = {new IntWritable(r0), new IntWritable(r1), new IntWritable(r0 + r1)};
                    IntArrayWritable arrayWritable = new IntArrayWritable(arr);
                    context.write(currGram, arrayWritable);
                    r0 = 0;
                    r1 = 0;
                    currGram = key;
                }
                else {
                    for (IntArrayWritable currValue : values) {
                        if (currValue.get()[0].get() == 0)
                            r0 += currValue.get()[1].get();
                        else
                            r1 += currValue.get()[1].get();
                    }
                }
            }

            /**
             * writes the last ngram's details
             */
            @Override
            protected void cleanup(Reducer<Text, IntArrayWritable, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException{
                IntWritable[] arr = {new IntWritable(r0), new IntWritable(r1), new IntWritable(r0 + r1)};
                IntArrayWritable contextValue = new IntArrayWritable(arr);
                context.write(currGram, contextValue);
                super.cleanup(context);
            }
        }

        /**
         * partitions the keys so that each ngram will go to the same reducer.
         */
        public static class PartitionerClass extends Partitioner<Text, IntArrayWritable> {
            public int getPartition(Text key, IntArrayWritable values, int numOfPartitions) {
                return key.hashCode() % numOfPartitions;
            }
        }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Template");
        job.setJarByClass(OccurrencesCounter.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class); //todo find out how to write the writable array

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
