/*
 * This mapper class code is used by Reduce workers.
 * Reduce operation is done by using selected statistical function. (min, max, mean, std dev, mode)
 * Responsible: Cetin Tekin
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.swing.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.Math;
import java.util.List;

public class StatisticalReducer {

    /* Reducing is done by taking the mean of the star rating values */
    public static class MeanReducer
            extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<Double> valuesAsList;

            /* Converting iterator to list */
            valuesAsList = convertIteratorToList(values);

            /* Calculating sum and count of the star rating values for the current key */
            ArrayList<Double> sumAndCount =  StatisticalReducer.calculateSumAndCount(valuesAsList);

            double sum = sumAndCount.get(0);
            double count = sumAndCount.get(1);

            result.set(sum/count);
            context.write(key, result);

            /* Setting the current reducer's progress in GUI */
            //MainProgram.guiForm.setReducerProgress(context.getProgress());
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

            /* Setting the current reducer's progress in GUI */
            //MainProgram.guiForm.setReducerProgress(context.getProgress());
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

            /* Setting the current reducer's progress in GUI */
            //MainProgram.guiForm.setReducerProgress(context.getProgress());
        }
    }

    /* Reducing is done by taking the std deviation of the star rating values */
    public static class StdDevReducer
            extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            ArrayList<Double> sumAndCount;
            List<Double> valuesAsList;
            double mean;
            double count;
            double sumSquaredDiff;      /* Holds sum of the squared differences */
            double stdDev;              /* The std dev */


            /* Getting values to a list from the iterator */
            valuesAsList = convertIteratorToList(values);


            /* Calculating sum and count of the key-val pairs */
            sumAndCount = StatisticalReducer.calculateSumAndCount(valuesAsList);
            count = sumAndCount.get(1);
            mean = sumAndCount.get(0)/count;
            sumSquaredDiff = 0.0;
            stdDev = 0.0;


            /* Calculating sum of the squared differences */
            for (int i=0;i<valuesAsList.size();i++) {
                System.out.println(valuesAsList.get(i));
                sumSquaredDiff += Math.pow(valuesAsList.get(i) - mean, 2);
            }


            /* Calculating standard deviation */
            if (count != 1) {
                stdDev = Math.sqrt(sumSquaredDiff/(count-1));
            }

            result.set(stdDev);
            context.write(key, result);

            /* Setting the current reducer's progress in GUI */
            //MainProgram.guiForm.setReducerProgress(context.getProgress());
        }
    }

    /* Reducing is done by taking the mode of the star rating values */
    public static class ModeReducer
            extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int[] frequencyArray = {0,0,0,0,0};     /* Holds the frequency of each star rating value (1 to 5) */
            int maxFreqIndex;

            /* Calculating frequency of each star rating value */
            for (DoubleWritable val : values) {
                frequencyArray[((int)val.get())-1]++;
            }

            maxFreqIndex = 0;

            /* Finding maximum of the frequencies */
            for(int i=1;i<5;i++) {
                if (frequencyArray[i] > frequencyArray[maxFreqIndex]) {
                    maxFreqIndex = i;
                }
            }
            result.set(maxFreqIndex+1);
            context.write(key, result);

            /* Setting the current reducer's progress in GUI */
            //MainProgram.guiForm.setReducerProgress(context.getProgress());
        }
    }

    /* Reducing is done by counting the total number of votes for a given product name */
    public static class CountReducer
            extends Reducer<Text, DoubleWritable,Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numVotes;

            numVotes = StatisticalReducer.convertIteratorToList(values).size();
            result.set(numVotes);
            context.write(key, result);

            /* Setting the current reducer's progress in GUI */
            //MainProgram.guiForm.setReducerProgress(context.getProgress());
        }
    }

    /* this method returns sum and number of elements in the given iterable */
    private static ArrayList<Double> calculateSumAndCount(List<Double> values) {
        ArrayList<Double> sumAndCount = new ArrayList<Double>();        /* Sum is stored in first index, count is in second index */
        double sum = 0.0;
        double count = 0.0;


        for (int i=0;i<values.size();i++) {
            sum += values.get(i);
            count = count + 1.0;
        }
        sumAndCount.add(sum);
        sumAndCount.add(count);

        return sumAndCount;

    }

    /* This method converts given iterator set to a list */
    private static List<Double> convertIteratorToList(Iterable<DoubleWritable> values) {

        List<Double> valuesAsList;

        valuesAsList = new ArrayList<Double>();

        while (values.iterator().hasNext()) {
            valuesAsList.add(values.iterator().next().get());
        }

        return valuesAsList;

    }


}




