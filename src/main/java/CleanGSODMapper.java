
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GSODCleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    public static Pattern p = Pattern.compile(".*GUST.*");
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher m = p.matcher(line);
        if (m.matches() != true) {
            context.write(NullWritable.get(), new Text(line));
        }
    }
}

