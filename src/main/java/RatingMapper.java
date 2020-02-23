/*
 * This mapper class code is used by Map workers.
 * Product names are mapped to the star ratings.
 * Responsible: Cetin Tekin
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class RatingMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    private DoubleWritable starRating = new DoubleWritable();
    private Text word = new Text();

    /* Map function runs for each line in the file. Maps each product name to star rating. */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String valueStr = value.toString();
        /* Parsing the record */
        String[] columns = valueStr.split("\\t");

        /* Getting the 5th column of the current line (product_title) */
        word.set(columns[5]);
        /* Getting the 7th column of the current line (star_rating) */
        starRating.set(Double.parseDouble(columns[7]));

        context.write(word, starRating);

    }
}