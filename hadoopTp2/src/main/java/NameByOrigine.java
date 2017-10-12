import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


// we can't use a combiner because the output of the reducer is a map so not the same type as the value in input

public class NameByOrigine {


    /*
    * value: takes the String that is a line 'key' of the csv file.
    * The map()function returns, for each Origins, as key the origin and as value the name
    * example: Marie;f;french,english;10
    *           maps: (french,Marie) and (english,Marie)
    * */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer1 = new StringTokenizer(line,";");
                String name = tokenizer1.nextToken();
                tokenizer1.nextToken();
                StringTokenizer tokenizer2 = new StringTokenizer(tokenizer1.nextToken(),",");
                while (tokenizer2.hasMoreTokens()) {
                    word.set(name);
                    context.write(new Text(tokenizer2.nextToken()), word);
                }


        }
    }


    //The shuffle returns for each origin the list of name that are associated with it
    //example: (french, [Marie,Lou,Ben,...]

    /*
    *Key and value take the result of the shuffle
    * The result of the reduce function is
    * for each origin the number of time that the name appears
    * example:
    * if the shuffle returns for the origin 'french' (french, [Marie, Alex, Marie, Laura]
    * the output will be (french, [[Marie,2],[Alex,1],[Laura,1]]
    */

    public static class Reduce extends Reducer< Text, Text, Text, MapWritable> {
        private  MapWritable tab = new MapWritable();
        private Text name=new Text();
        public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException {
            long val;
            while (values.hasNext()) {
                name=values.next();
                if(tab.get(name)==null){
                    tab.put(name,new LongWritable(0));
                }else{
                    LongWritable nb = (LongWritable)tab.get(name);
                    val =nb.get();
                    tab.put(name, new LongWritable(val+1));
                }
            }
            context.write(key, tab);
        }
    }

}
