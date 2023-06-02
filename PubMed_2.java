import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class PubMed_2 {
    public static class JournamNameMapper
            extends Mapper<LongWritable, Text, Text, NullWritable>{

       // private final static IntWritable NULL = new IntWritable(0);
        private Text journalName = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String file_row = value.toString().trim();
            String [] tokens = file_row.split("\t");
            journalName.set(tokens[7].trim());
            context.write(journalName, NullWritable.get());
        }
    }

    public static class JournamNameReducer
            extends Reducer<Text,NullWritable,Text,NullWritable> {
       // private final static Text result = new Text();

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PubMed_2");
        job.setJarByClass(PubMed_2.class);
        job.setMapperClass(PubMed_2.JournamNameMapper.class);
        job.setCombinerClass(PubMed_2.JournamNameReducer.class);
        job.setReducerClass(PubMed_2.JournamNameReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
