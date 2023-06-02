import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PubMed_3 {

    public static class PublicationCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text journal = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String file_row = value.toString().trim();
            String year = context.getConfiguration().get("Year");
            String [] tokens = file_row.split("\t");
            if(tokens[6].trim().equals(year)){
                journal.set(tokens[7].trim());
                context.write(journal, one);
            }


        }
    }

    public static class PublicationCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("Year",args[0]);
        Job job = Job.getInstance(conf, "PubMed_3");
        job.setJarByClass(PubMed_3.class);
        job.setMapperClass(PubMed_3.PublicationCountMapper.class);
        job.setCombinerClass(PubMed_3.PublicationCountReducer.class);
        job.setReducerClass(PubMed_3.PublicationCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
