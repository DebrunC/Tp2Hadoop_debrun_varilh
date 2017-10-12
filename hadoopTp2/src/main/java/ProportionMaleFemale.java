import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


// we can't use a combiner because the result depends on the total
public class ProportionMaleFemale {


    /*
    * value: takes the String that is a line 'key' of the csv file.
    * The map()function returns as key the gender and as value 1 if the person was of this gender and 0 otherwise
    * example: Marie;f;french,english;10
    *           maps: (f,1)  and (m,0) because Marie's gender is female and not male
    * */
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        LongWritable one =new LongWritable(1);
        LongWritable zero =new LongWritable(0);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,";");
                tokenizer.nextToken();
                StringTokenizer tokenizer2 = new StringTokenizer(tokenizer.nextToken(),",");
                while (tokenizer2.hasMoreTokens()) {
                    if(tokenizer2.nextToken().equals('m')){
                        context.write(new Text("m"),one);
                        context.write(new Text("f"),zero);
                    }else{
                        context.write(new Text("f"),one);
                        context.write(new Text("m"),zero);
                    }
                }


        }
    }

    //The shuffle returns for each gender the list of 0 or 1 which indicate that the person was of the same gender or not as the key
    //example: (f, [0,1,0,0,1,1]

    /*
    *Key and value take the result of the shuffle
    * The result of the reduce function is
    * for each gender the percentage of people of the gender of the key(f or m)
    * example:
    * if the shuffle returns for gender 'f' (f, [0,1,0,0,1,1]
    * the output will be (f, 50) 50=3*100/6  50% of the csv file are female
    */

    public static class Reduce extends Reducer< Text,LongWritable,  Text,FloatWritable> {
        private MapWritable tab = new MapWritable();
        public void reduce(Text key, Iterator<LongWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int total=0;
            while (values.hasNext()) {
                total++;
                count+= values.next().get();
            }
            context.write(key, new FloatWritable(count*100/total));
        }
    }

}