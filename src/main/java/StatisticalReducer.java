/*
 * This mapper class code is used by Reduce workers.
 * Reduce operation is done by using selected statistical function. (min, max, mean, std dev, mode)
 * Responsible: Cetin Tekin
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class StatisticalReducer {

    /* Reducing is done by taking the mean of the star rating values */
    public static class MeanReducer
            extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }

    /* Reducing is done by taking the minimum of the star rating values */
    public static class MinReducer
            extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Iterator itr = values.iterator();
            double min = ((DoubleWritable) itr.next()).get();
            for (DoubleWritable val : values) {
                if (val.get() < min)
                    min = val.get();
            }
            result.set(min);
            context.write(key, result);
        }
    }

    /* Reducing is done by taking the maximum of the star rating values */
    public static class MaxReducer
            extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Iterator itr = values.iterator();
            double max = ((DoubleWritable) itr.next()).get();
            for (DoubleWritable val : values) {
                if (val.get() > max)
                    max = val.get();
            }
            result.set(max);
            context.write(key, result);
        }
    }


}




