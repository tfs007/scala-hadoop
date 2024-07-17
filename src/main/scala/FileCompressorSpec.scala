import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileCompressorSpec extends AnyFlatSpec with Matchers {
  "FileCompressorMapper" should "extract file name correctly" in {
    val mapper = new FileCompressorMapper()
    val driver = new MapReduceDriver[Object, BytesWritable, Text, BytesWritable]()
      .withMapper(mapper)
      .withInputKey(new Object())
      .withInputValue(new BytesWritable("Hello, world!".getBytes))

    val output = driver.run()
    output should contain (new Text("large_file.txt"), new BytesWritable("Hello, world!".getBytes))
  }

  "FileCompressorReducer" should "compress data correctly" in {
    val reducer = new FileCompressorReducer()
    val driver = new MapReduceDriver[Text, BytesWritable, Text, BytesWritable]()
      .withReducer(reducer)
      .withInputKey(new Text("large_file.txt"))
      .withInputValues(List(new BytesWritable("Hello, world!".getBytes)).asJava)

    val output = driver.run()
    output should contain (new Text("large_file.txt"), new BytesWritable("Hello, world!".getBytes))
  }
}
