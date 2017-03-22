import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class Gdelt extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TitleCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "gdelt");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(GdeltMap.class);
        job.setReducerClass(GdeltReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(TitleCount.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }


    public static class GdeltMap extends Mapper<Object, Text, Text, IntWritable> {
        List<String> violence_codes;
        List<String> peace_codes;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String violenceCodesPath = conf.get("voilence");
            String violenceCodesPath = conf.get("peace");

            this.violence_codes = Arrays.asList(readHDFSFile(violenceCodesPath, conf).split("\n"));
            this.peace_codes = Arrays.asList(readHDFSFile(peaceCodesPath, conf).split("\n"));
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String event = value.toString().split("\t");
            String code = event[26];
            System.out.println("CODE: " + code);

            if(violence_codes.contains(code)){
              context.write(new Text("violence"), new IntWritable(1));
            } else if (peace_codes.contains(code)) {
              context.write(new Text("peace"), new IntWritable(1));
            } else{

            }

        }
    }

    public static class GdeltReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values){
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }
}
