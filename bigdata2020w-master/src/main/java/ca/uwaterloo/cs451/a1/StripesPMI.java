/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;



import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.map.HashMapWritable;
import tl.lin.data.map.MapKF;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloats;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.io.*;
import java.lang.*;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

/**
 * Simple word count demo.
 */
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

 


  public static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

	List<String> tokens = Tokenizer.tokenize(value.toString());
	List<String> buf=new ArrayList();
        for (int i=0;i<Math.min(40,tokens.size());i++) {
	  if(!buf.contains(tokens.get(i))) {
	    WORD.set(tokens.get(i));
            context.write(WORD, ONE);
	    buf.add(tokens.get(i));
	}
      }
        WORD.set("*");
	context.write(WORD, ONE);
    }
  }

  

  // Reducer: sums up all the counts.
  public static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();
     private int threshold=-1;

    @Override
    public void setup(Context context) {

     threshold = context.getConfiguration().getInt("threshold", 2);

    }


    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      if(sum>=threshold||key.toString().equals("*")) {
      SUM.set(sum);
      context.write(key, SUM);
      }
    }
  }


 public static final class PMIMapper extends Mapper<LongWritable, Text, Text, HashMapWritable<Text,PairOfFloats>> {
 
   
   private final static Text TEXT=new Text();
   @Override
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      
      List<String> tokens = Tokenizer.tokenize(value.toString());
      Map<String, HashMapWritable<Text,PairOfFloats>> stripes = new HashMap<>();
      int mx=Math.min(40,tokens.size());
      
        for (int i=0; i<mx; i++) {
          String tar=tokens.get(i);
	  if(!stripes.containsKey(tar)) {
            HashMapWritable<Text,PairOfFloats> stripe= new HashMapWritable<Text,PairOfFloats>();
            for (int j=0;j<mx;j++) {
	      if(!tokens.get(i).equals(tokens.get(j))) {	    
               
              stripe.put(new Text(tokens.get(j)), new PairOfFloats(1.0f,1.0f));
	      }
	    }
              stripes.put(tokens.get(i), stripe);  
       
	  }
	}

      for (String t : stripes.keySet()) {
        TEXT.set(t);
        context.write(TEXT, stripes.get(t));
      }

   
   
   } 
 }





  private static final class PMICombiner extends Reducer<Text, HashMapWritable<Text,PairOfFloats>, Text, HashMapWritable<Text,PairOfFloats>> {
    @Override
    public void reduce(Text key, Iterable<HashMapWritable<Text,PairOfFloats>> values, Context context)
        throws IOException, InterruptedException {
      HashMapWritable<Text,PairOfFloats> map = new HashMapWritable<Text,PairOfFloats>();
      Iterator iter1=values.iterator();
      while(iter1.hasNext()) {
       HashMapWritable<Text,PairOfFloats> bla=(HashMapWritable<Text,PairOfFloats>)iter1.next();
      Set entries = bla.entrySet();
      Iterator iter = entries.iterator();
      while(iter.hasNext()){
       Map.Entry entry = (Map.Entry)iter.next();
       Text t = (Text)entry.getKey();
       float val = ((PairOfFloats)(entry.getValue())).getValue();

       if(map.containsKey(t)) {
       
       float orig=map.get(t).getValue();
       map.put(new Text (t),new PairOfFloats(val+orig,val+orig));
       } else {
       map.put(new Text (t),new PairOfFloats(val,val));
       }
      }
      }

      

      context.write(key, map);
    }
  }




   public static final class PMIReducer extends Reducer<Text, HashMapWritable<Text,PairOfFloats>, Text, HashMapWritable<Text,PairOfFloats>> {
     private static final PairOfFloats PMI=new PairOfFloats();
     private Map<String, Float> prevmap= new HashMap<String, Float>();
     private static final Text TKey=new Text();
     private float Lines=0;
     private int threshold=-1;


 @Override

     public void setup(Context context) throws IOException, InterruptedException {
       Configuration conf = context.getConfiguration();
       FileSystem fs = FileSystem.get(conf);
       threshold = conf.getInt("threshold", 2);
       FileStatus[] stat = fs.listStatus(new Path("A1-inter-Pairs/"));
        for (int i=0; i < stat.length; i++) {
          BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stat[i].getPath()),"UTF-8"));
          String line = br.readLine();
          while(line != null){
          String[] tokens = line.split("\\s+");
          prevmap.put(tokens[0], Float.valueOf(tokens[1]));
	  line = br.readLine();
          }
        }



       Lines=prevmap.get("*");


     }















    


     @Override
   
     public void reduce(Text key, Iterable<HashMapWritable<Text,PairOfFloats>> values,  Context context )
              throws IOException, InterruptedException {
                 float sum=0.f;
		 
		 float right=0.f;
		 
                 HashMapWritable<Text,PairOfFloats> map = new HashMapWritable<Text,PairOfFloats>();
                 Iterator iter1=values.iterator();
                 
                 while(iter1.hasNext()) {
                    HashMapWritable<Text,PairOfFloats> bla=(HashMapWritable<Text,PairOfFloats>)iter1.next();
                    Set entries = bla.entrySet();
                    Iterator iter = entries.iterator();
                     while(iter.hasNext()){
                       Map.Entry entry = (Map.Entry)iter.next();
                       Text t = (Text)entry.getKey();
                       float val = ((PairOfFloats)(entry.getValue())).getValue();

                       if(map.containsKey(t)) {

                         float orig=map.get(t).getValue();
                         map.put(new Text (t),new PairOfFloats(val+orig,val+orig));
                      } else {
                        map.put(new Text (t),new PairOfFloats(val,val));
                      }
                     }
                 }
                
                List<Text> drop=new ArrayList<Text>();
                for(Text t: map.keySet()) {
		  
		  sum=map.get(t).getKey();
		  
		  if(sum>=threshold) {
	          right=prevmap.get(t.toString());
		   float left=prevmap.get(key.toString());
 
		  double result=sum*Lines/left/right;
		  float  reresult=(float)(Math.log10(result));
		  PMI.set(reresult,sum);
		  TKey.set(t);
		  map.put(new Text(TKey),new PairOfFloats(PMI.getKey(),PMI.getValue()));
		  
		}else {
		  drop.add(t);
		}
		}

		for(Text t:drop) {
		map.remove(t);
		}
		
                

                
                context.write(key,map);
		

     
     
     
     }
   }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
    @Option(name = "-cb", usage = "use  combiner")
    boolean cb = false;
    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold")
    int threshold = -1;

  
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName()+"first stage");
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" -use combiner: " + args.cb);
    LOG.info(" - threshold: " + args.threshold);
    String interpath="A1-inter-Pairs";

    Configuration conf = getConf();
    Job job1 = Job.getInstance(conf);
    job1.setJobName(StripesPMI.class.getSimpleName());
    job1.setJarByClass(StripesPMI.class);
    
    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(interpath));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(TextOutputFormat.class);
    job1.getConfiguration().setInt("threshold", args.threshold);

    job1.setMapperClass( MyMapper.class);
    if(args.cb) {
    job1.setCombinerClass(MyReducer.class);
    }
    job1.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path interDir = new Path(interpath);
    FileSystem.get(conf).delete(interDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

   
   
    LOG.info("Tool: " + StripesPMI.class.getSimpleName()+"second stage");
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - not use combiner " + args.cb);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf2=getConf(); 
    Job job2 = Job.getInstance(conf2);
    job2.setJobName(StripesPMI.class.getSimpleName());
    job2.setJarByClass(StripesPMI.class);
    job2.getConfiguration().setInt("threshold", args.threshold);
    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(HashMapWritable.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(HashMapWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");
    job2.setMapperClass( PMIMapper.class);
    if(args.cb) {
    job2.setCombinerClass(PMICombiner.class);
    }
    job2.setReducerClass(PMIReducer.class);

    // Delete the output directory if it exists already.
    Path outDir = new Path(args.output);
    FileSystem.get(conf).delete(outDir, true);


    long startTime2 = System.currentTimeMillis();


    job2.waitForCompletion(true);
    LOG.info("Job2 Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    LOG.info("Total time: " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
