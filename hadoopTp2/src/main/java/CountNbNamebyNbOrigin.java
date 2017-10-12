import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


// we can use a combiner because the result is commutative and does not depend on a total
public class CountNbNamebyNbOrigin {

    /*
    * value: takes the String that is a line 'key' of the csv file.
    * The map()function returns, for each number of Origins, as key the number of origin and as value the name
    * example: Marie;f;french,english;10
    *           maps: (2,Marie) because Marie is a name that has 2 origins
    * */
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            long count =0;
            String line = value.toString();
            StringTokenizer tokenizer1 = new StringTokenizer(line,";");

                String name = tokenizer1.nextToken();
                tokenizer1.nextToken();
                StringTokenizer tokenizer2 = new StringTokenizer(tokenizer1.nextToken(),",");
                while (tokenizer2.hasMoreTokens()) {
                    count++;
                    tokenizer2.nextToken();
                }
                word.set(name);
                context.write(new LongWritable(count), word);


        }
    }


    //The shuffle returns for each number of origin the list of name that are associated with it
    //example: (2, [Marie,Lou,Ben,...]

    /*
    *Key and value take the result of the shuffle
    * The result of the reduce function is
    * for each number of origin the number of different name that there are
    * example:
    * if the shuffle returns for the number of origin '2' (2, [Marie, Alex, Marie, Laura]
    * the output will be (2, 3)
    * Indeed there are 3 names that have 2 origins
    */
    public static class Reduce extends Reducer< LongWritable, Text, LongWritable,LongWritable> {
        private  MapWritable tab = new MapWritable();
        public void reduce(LongWritable key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
            while (values.hasNext()) {
                tab.put(values.next(),new LongWritable(0));
            }
            context.write(key, new LongWritable(tab.size()));
        }
    }

}
