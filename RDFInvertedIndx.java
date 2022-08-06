package uk.ac.man.cs.comp38211.exercise;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.Property;

import uk.ac.man.cs.comp38211.io.array.ArrayListWritable;
import uk.ac.man.cs.comp38211.io.pair.PairOfStrings;
import uk.ac.man.cs.comp38211.io.pair.PairOfWritables;
import uk.ac.man.cs.comp38211.ir.StopAnalyser;
import uk.ac.man.cs.comp38211.util.XParser;

public class RDFInvertedIndex extends Configured implements Tool
{
    private static final Logger LOG = Logger
            .getLogger(RDFInvertedIndex.class);

    public static class Map extends 
            Mapper<LongWritable, Text, Text, PairOfWritables<Text,Text>>
    {        

        protected Text document = new Text();
        //protected PairOfStrings predobj = new PairOfStrings();
        protected Text term = new Text();
        protected Text pred = new Text();
        protected Text subj = new Text();
        
        @SuppressWarnings("unused")
        private StopAnalyser stopAnalyser = new StopAnalyser();
        
        //protected Set<String> long_text_tokens = Set.of("abstract","comment","quote","couchGag","imageCaption","blackboard");
        
        public String getTerm(String uri)
        {
        	int slash = uri.lastIndexOf('/');
        	int hash = uri.lastIndexOf('#');
        	if((slash < hash && uri.substring(slash + 1, hash).equals("XMLSchema")) || (slash != -1 && uri.substring(uri.substring(0, slash).lastIndexOf('/') + 1,slash).equals("datatype")))
        	{
        		return uri.substring(0,uri.indexOf('^'));
        	}
        	int last_symbol = Math.max(slash, hash);
        	String term = uri.substring(last_symbol + 1).replace('_', ' ');
        	// Removing language tags
        	if(uri.lastIndexOf('@') > uri.lastIndexOf('.'))
        	{
        		return term.substring(0,uri.lastIndexOf('@'));
        	}
        	if(term.length() >= 4 && term.substring(term.length() - 4).equals(".jpg"))
        	{
        		return term.substring(0,term.length() - 4);
        	}
        	return term;
        }
        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {            
            // This statement ensures we read a full rdf/xml document in
            // before we try to do anything else
            if(!value.toString().contains("</rdf:RDF>"))
            {    
                document.set(document.toString() + value.toString());
                return;
            }
            
            // We have to convert the text to a UTF-8 string. This must be
            // enforced or errors will be thrown. 
            String contents = document.toString() + value.toString();
            contents = new String(contents.getBytes(), "UTF-8");
            
            // The string must be cast to an inputstream for use with jena
            InputStream fullDocument = IOUtils.toInputStream(contents);
            document = new Text();
            
            // Create a model
            Model model = ModelFactory.createDefaultModel();
            
            try
            {
                model.read(fullDocument, null);
            
                StmtIterator iter = model.listStatements();
            
                // Iterate over all the triples, set and output them
                while(iter.hasNext())
                {
                    Statement stmt      = iter.nextStatement();
                    Resource  subject   = stmt.getSubject();
                    Property  predicate = stmt.getPredicate();
                    RDFNode   object    = stmt.getObject();
                    
                    boolean in_quote = false;
                    String quote = "";
                    //term.set(getTerm(object.toString()), getTerm(predicate.toString()));
                    term.set(getTerm(object.toString()));
                    pred.set(getTerm(predicate.toString()));
                    subj.set(subject.toString());
                    String[] words = term.toString().split("\\s+");
                    if(words.length > 5)//long_text_tokens.contains(pred.toString()))
                    {
                    	StringTokenizer itr = new StringTokenizer(term.toString());
                    	while (itr.hasMoreTokens())
                    	{
                    		String token = itr.nextToken();
                    		token = token.replaceAll("\\(|\\,|\\)|\\.|\\[|\\]|\\{|\\}|\\;", "");
                    		
                    		if(!in_quote && token.indexOf('"') == -1)
                    		{
                        		if(!stopAnalyser.isStopWord(token.toLowerCase()))
                    			{
                    				term.set(token);
                            		context.write(term, new PairOfWritables(pred,subj));
                    			}
                    		}
                    		if(token.indexOf('"') != token.lastIndexOf('"') && token.indexOf('"') != -1)
                    		{
                    			token = token.substring(1,token.length()-1);
                    			if(!stopAnalyser.isStopWord(token.toLowerCase()))
                    			{
                    				term.set(token);
                            		context.write(term, new PairOfWritables(pred,subj));
                    			}
                    		}else {
                    			if(in_quote && token.lastIndexOf('"') == -1)
                    			{
                    				quote += " ";
                            		quote += token;
                    			}
                    			if(in_quote && token.lastIndexOf('"') != -1)
                    			{
                    				quote += " ";
                    				token = token.substring(0,token.length()-1);
                    				quote += token;
                    				quote = quote.substring(1);
                    				term.set(quote);
                            		context.write(term, new PairOfWritables(pred,subj));
                            		quote = "";
                            		in_quote = false;
                    			} 
                    			if(!in_quote && token.indexOf('"') == 0)
                    			{
                    				in_quote = true;
                    				token = token.substring(1);
                    				quote += " ";
                            		quote += token;
                    			}
                    			                   				
                    		}
                    	}
                    }
                    else {
                    	context.write(term, new PairOfWritables(pred,subj));
                    }
                    
                }
            }
            catch(Exception e)
            {
                LOG.error(e);
            }
        }
    }

    public static class Reduce extends Reducer<Text, PairOfWritables<Text,Text>, Text, ArrayListWritable<PairOfWritables<Text,Text>>>
    {
       
        // This reducer turns an iterable into an ArrayListWritable, sorts it
        // and outputs it
        public void reduce(
                Text key,
                Iterable<PairOfWritables<Text,Text>> values,
                Context context) throws IOException, InterruptedException
        {
            ArrayListWritable<PairOfWritables<Text,Text>> postings = new ArrayListWritable<PairOfWritables<Text,Text>>();
            
            //Iterator<PairOfWritables<Text,Text>> iter = values.iterator();
            
            //while(iter.hasNext()) {
            	//Text copy = new Text(iter.next());
                //postings.add(copy);
            //}
            for(PairOfWritables<Text,Text> name : values) {
        		if (!postings.contains(name))
        		{
        			//filenames.add(new Text(name.getLeftElement()));
        			postings.add(new PairOfWritables(name.getLeftElement(), name.getRightElement()));
        		}
        	}
            
            //Collections.sort(postings);
            
            context.write(key, postings);
        }
    }

    public RDFInvertedIndex()
    {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception
    {        
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline = null;
        CommandLineParser parser = new XParser(true);

        try
        {
            cmdline = parser.parse(options, args);
        }
        catch (ParseException exp)
        {
            System.err.println("Error parsing command line: "
                    + exp.getMessage());
            System.err.println(cmdline);
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT))
        {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }      
        
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        Job RDFIndex = new Job(new Configuration());

        RDFIndex.setJobName("Inverted Index 1");
        RDFIndex.setJarByClass(RDFInvertedIndex.class);        
        RDFIndex.setMapperClass(Map.class);
        RDFIndex.setReducerClass(Reduce.class);
        RDFIndex.setMapOutputKeyClass(Text.class);
        RDFIndex.setMapOutputValueClass(PairOfWritables.class);
        RDFIndex.setOutputKeyClass(Text.class);
        RDFIndex.setOutputValueClass(ArrayListWritable.class);
        FileInputFormat.setInputPaths(RDFIndex, new Path(inputPath));
        FileOutputFormat.setOutputPath(RDFIndex, new Path(outputPath));

        long startTime = System.currentTimeMillis();
       
        RDFIndex.waitForCompletion(true);
        if(RDFIndex.isSuccessful())
            LOG.info("Job successful!");
        else
            LOG.info("Job failed.");
        
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new RDFInvertedIndex(), args);
    }
}
