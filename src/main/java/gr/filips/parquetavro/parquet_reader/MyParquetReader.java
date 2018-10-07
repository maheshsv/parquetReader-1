package gr.filips.parquetavro.parquet_reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

/**
 * read parquet
 *
 */
public class MyParquetReader {
	public static void main(String[] args) {
		System.out.println("Read Parquet");
		MyParquetReader app = new MyParquetReader();
		/*org.apache.hadoop.fs.Path path = new Path(pathString);
		org.apache.hadoop.conf.Configuration conf;
		app.theReader(HadoopInputFile.fromPath(path, conf));*/
		
		InputFile inputFile = null;
		try {
			inputFile = TideWorksInputFile.nioPathToInputFile(FileSystems.getDefault().getPath("demo.parquet"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		app.theReader(inputFile);
	}

	public void theReader(InputFile inputFile) {
		try {
			ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build();
			GenericRecord nextRecord = reader.read();
			while (nextRecord != null) {
				System.out.println("rec=" + nextRecord.toString());
				/*System.out.println("value by name c1=" + nextRecord.get("c1"));
				System.out.println("value by name c2=" + nextRecord.get("c2"));*/
				nextRecord = reader.read();
			} 
			
			//2nd method of reading
//			ParquetFileReader pfr = ParquetFileReader.open(inputFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			
		}
	}
	
	/*public static class Builder extends ParquetWriter.Builder<Group, Builder> {
	    private MessageType type = null;
	    private Map<String, String> extraMetaData = new HashMap<String, String>();

	    private Builder(Path file) {
	      super(file);
	    }

	    public Builder withType(MessageType type) {
	      this.type = type;
	      return this;
	    }

	    public Builder withExtraMetaData(Map<String, String> extraMetaData) {
	      this.extraMetaData = extraMetaData;
	      return this;
	    }

	    @Override
	    protected Builder self() {
	      return this;
	    }

	    @Override
	    protected WriteSupport<Group> getWriteSupport(Configuration conf) {
	      return new GroupWriteSupport(type, extraMetaData);
	    }

	  }*/
}
