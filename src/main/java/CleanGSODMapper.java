import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Arpit on 2/19/2015.
 */
public class CleanGSODMapper extends Mapper<IntWritable, Text, Text, Text> {

        protected static Pattern p = Pattern.compile("STN--.*");
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher m = p.matcher(line);
            if (!m.matches()){
                context.write(new Text(line), null);
            }

    }

}
