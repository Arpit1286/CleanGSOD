import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;


/**
 * Created by Arpit on 2/19/2015.
 */
public class CleanGSOD {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2){
            System.err.println("Usage:CleanGSOD <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(CleanGSOD.class);
        job.setJobName("CleanGSOD");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CleanGSODMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

