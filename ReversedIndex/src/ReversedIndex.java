import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class ReversedIndex {

    public static class OccurrenceMapper extends Mapper<LongWritable, Text, Text, LongEntry> {

        private List<String> stopWords = new ArrayList<>();
        private LongEntry wordOccurrence = new LongEntry();
        private Text fileNameKey = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            Path filePath = new Path(cacheFiles[0].toString());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] words = line.split(" ");
                stopWords.addAll(Arrays.asList(words));
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            StringTokenizer iterator = new StringTokenizer(value.toString(), "\"\',.()?![]#$*-;:_+/\\<>@%& ");

            while (iterator.hasMoreTokens()) {
                String word = iterator.nextToken();
                if (!stopWords.contains(word)) {
                    fileNameKey.set(fileName);
                    wordOccurrence.setKey(word);
                    wordOccurrence.setValue(key.get());
                    context.write(fileNameKey, wordOccurrence);
                }
            }
        }
    }

    public static class OccurrenceReducer extends Reducer<Text, LongEntry, Text, Text> {

        private Text wordOccurrence = new Text();

        @Override
        protected void reduce(Text key, Iterable<LongEntry> values, Context context) throws IOException, InterruptedException {
            List<LongEntry> valuesCache = new ArrayList<>();
            List<Long> offsets = new ArrayList<>();
            for (LongEntry value : values) {
                offsets.add(value.getValue().get());
                valuesCache.add(value.copy());
            }
            Map<Long, Long> offsetsToLines = mapOffsetsToLines(offsets);

            for (LongEntry pair : valuesCache) {
                String fileName = key.toString();
                Long lineNumber = offsetsToLines.get(pair.getValue().get());
                wordOccurrence.set(fileName + ":" + lineNumber);
                context.write(pair.getKey(), wordOccurrence);
            }
        }

        private Map<Long, Long> mapOffsetsToLines(List<Long> offsets) {
            Collections.sort(offsets);
            Map<Long, Long> offsetsMap = new HashMap<>();
            long lineCount = 0;
            for (Long offset : offsets) {
                if (!offsetsMap.containsKey(offset)) {
                    offsetsMap.put(offset, lineCount++);
                }
            }

            return offsetsMap;
        }
    }

    public static class IndexMapper extends Mapper<Text, Text, Text, LongEntry> {

        private LongEntry wordOccurrence = new LongEntry();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int splitIndex = value.toString().indexOf(":");
            String fileName = value.toString().substring(0, splitIndex);
            Long lineNumber = Long.parseLong(value.toString().substring(splitIndex + 1));
            wordOccurrence.setKey(fileName);
            wordOccurrence.setValue(lineNumber);
            context.write(key, wordOccurrence);
        }
    }

    public static class IndexReducer extends Reducer<Text, LongEntry, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<LongEntry> values, Context context) throws IOException, InterruptedException {
            Map<String, Set<Long>> occurrencesInFiles = extractOccurrencesInFiles(values);
            String occurrencesOutput = generateWordOccurrences(occurrencesInFiles);
            context.write(key, new Text(occurrencesOutput));
        }

        private Map<String, Set<Long>> extractOccurrencesInFiles(Iterable<LongEntry> entries) {
            Map<String, Set<Long>> occurrencesInFile = new HashMap<>();
            for (LongEntry value : entries) {
                String fileName = value.getKey().toString();
                Long lineNumber = value.getValue().get();
                if (occurrencesInFile.containsKey(fileName)) {
                    occurrencesInFile.get(fileName).add(lineNumber);
                } else {
                    Set<Long> lines = new HashSet<>();
                    lines.add(lineNumber);
                    occurrencesInFile.put(fileName, lines);
                }
            }

            return occurrencesInFile;
        }

        private String generateWordOccurrences(Map<String, Set<Long>> occurrencesInFile) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String fileName : occurrencesInFile.keySet()) {
                stringBuilder.append("(");
                stringBuilder.append(fileName);
                for (Long line : occurrencesInFile.get(fileName)) {
                    stringBuilder.append(", ");
                    stringBuilder.append(line);
                }
                stringBuilder.append(") ");
            }
            return stringBuilder.toString();
        }
    }

    private static void runOccurrencesMappingJob(String inputPath, String outputPath, String stopWordsPath)
            throws Exception {
        Configuration conf = new Configuration();
        Job occurrencesJob = Job.getInstance(conf, "Find occurrences");
        occurrencesJob.setJarByClass(ReversedIndex.class);
        occurrencesJob.setMapperClass(OccurrenceMapper.class);
        occurrencesJob.setReducerClass(OccurrenceReducer.class);
        occurrencesJob.setMapOutputKeyClass(Text.class);
        occurrencesJob.setMapOutputValueClass(LongEntry.class);
        occurrencesJob.setOutputKeyClass(Text.class);
        occurrencesJob.setOutputValueClass(Text.class);

//        Add list of stop words as distributed cache file
        try {
            occurrencesJob.addCacheFile(new URI(stopWordsPath));
        } catch (Exception e) {
            System.out.println("Failed to add cache file at path " + stopWordsPath);
            System.exit(1);
        }

        FileInputFormat.addInputPath(occurrencesJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(occurrencesJob, new Path(outputPath));

        if (!occurrencesJob.waitForCompletion(true)) {
            System.exit(1);
        }
    }

    private static void runIndexingJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job indexingJob = Job.getInstance(conf, "Inverted index");
        indexingJob.setJarByClass(ReversedIndex.class);
        indexingJob.setMapperClass(IndexMapper.class);
        indexingJob.setReducerClass(IndexReducer.class);
        indexingJob.setMapOutputKeyClass(Text.class);
        indexingJob.setMapOutputValueClass(LongEntry.class);
        indexingJob.setOutputKeyClass(Text.class);
        indexingJob.setOutputValueClass(Text.class);
        indexingJob.setInputFormatClass(KeyValueTextInputFormat.class);

        KeyValueTextInputFormat.addInputPath(indexingJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(indexingJob, new Path(outputPath));

        if (!indexingJob.waitForCompletion(true)) {
            System.exit(1);
        }

//        Cleanup intermediary output files
        Path tempPath = new Path(inputPath);
        FileSystem fs = tempPath.getFileSystem(conf);
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }
    }

    public static void main(String[] args) throws Exception {
        String jobPath = args[0];
        String stopWordsPath = args[1];
        String inputPath = jobPath + "/input";
        String outputPath = jobPath + "/output";
        String tempPath = jobPath + "/temp";
        runOccurrencesMappingJob(inputPath, tempPath, stopWordsPath);
        runIndexingJob(tempPath, outputPath);
        System.exit(0);
    }
}