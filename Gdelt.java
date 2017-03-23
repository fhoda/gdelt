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
        int res = ToolRunner.run(new Configuration(), new Gdelt(), args);
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

        job.setJarByClass(Gdelt.class);
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
            String peaceCodesPath = conf.get("peace");

            this.violence_codes = Arrays.asList(readHDFSFile(violenceCodesPath, conf).split("\n"));
            this.peace_codes = Arrays.asList(readHDFSFile(peaceCodesPath, conf).split("\n"));
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] event = value.toString().split("\t");
            String code = event[26];
            System.out.println("CODE: " + code);

            if(violence_codes.contains(code)){
              context.write(new Text("total_violence"), new IntWritable(1));
              if(code.startsWith("20")){
                context.write(new Text("UNCONVENTIONAL_MASS_VIOLENCE"), new IntWritable(1));
              }else if (code.startsWith("19")) {
                context.write(new Text("FIGHT"), new IntWritable(1));
              } else{
                context.write(new Text("ASSAULT"), new IntWritable(1));
              }
            } else if (peace_codes.contains(code)) {
              context.write(new Text("total_peace"), new IntWritable(1));
              if(code.startsWith("07")){
                context.write(new Text("PROVIDE_AID"), new IntWritable(1));
              } else if (code.startsWith("08")) {
                context.write(new Text("YIELD"), new IntWritable(1));
              } else if (code.startsWith("05")) {
                context.write(new Text("ENGAGE_IN_DIPLOMATIC_COOPERATION"), new IntWritable(1));
              } else{
                context.write(new Text("CONSULT"), new IntWritable(1));
              }
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
