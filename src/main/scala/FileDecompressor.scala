import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser

import java.io.IOException

object FileDecompressor {
  def main(args: Array[String]): Unit = {
    val (conf, otherArgs) = new GenericOptionsParser(new Configuration(), args).getConfiguration -> args.drop(1)
    if (otherArgs.length != 2) {
      System.err.println("Usage: FileDecompressor <input path> <output path>")
      System.exit(1)
    }

    val job = Job.getInstance(conf, "File Decompression")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[FileDecompressorMapper])
    job.setReducerClass(classOf[FileDecompressorReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[BytesWritable])
    FileInputFormat.addInputPath(job, new Path(otherArgs(0)))
    FileOutputFormat.setOutputPath(job, new Path(otherArgs(1)))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}

class FileDecompressorMapper extends Mapper[Object, BytesWritable, Text, BytesWritable] {
  @throws[IOException]
  @throws[InterruptedException]
  override def map(key: Object, value: BytesWritable, context: Mapper[Object, BytesWritable, Text, BytesWritable]#Context): Unit = {
    val fileName = new Text(context.getInputSplit.toString.split("/").last)
    context.write(fileName, value)
  }
}

class FileDecompressorReducer extends Reducer[Text, BytesWritable, Text, BytesWritable] {
  @throws[IOException]
  @throws[InterruptedException]
  override def reduce(key: Text, values: java.lang.Iterable[BytesWritable], context: Reducer[Text, BytesWritable, Text, BytesWritable]#Context): Unit = {
    val decompressedData = new BytesWritable(values.iterator().next().getBytes)
    context.write(key, decompressedData)
  }
}
