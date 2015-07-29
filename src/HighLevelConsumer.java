import java.io.IOException;
import java.io.PrintWriter;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class HighLevelConsumer implements Runnable {
	private KafkaStream m_stream;
	private int m_threadNumber;
	
	private static FileSystem fileSystem;
	private String dataDir;
	
	public HighLevelConsumer(KafkaStream a_stream, int a_threadNumber, String dataDir) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
		this.dataDir = dataDir;
		
		Configuration config = new Configuration();
		config.addResource("hdfs-default.xml");
		config.addResource("hdfs-site.xml");

		// This is required so that closing one instance of the HdfsClient doesn't close every instance.
		config.setBoolean("fs.hdfs.impl.disable.cache", true);

		try {
			fileSystem = FileSystem.get(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()) {
			String str = new String(it.next().message());
			System.out.println("Writing "+ str +" to hdfs");
			try {
				append(str);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		System.out.println("Shutting down Thread: " + m_threadNumber);
	}
	
	public void append(@Nonnull final String content) throws IOException {
		final Path path = new Path(this.dataDir);
		FSDataOutputStream fsout;
		if (!fileSystem.exists(path)) {
			fsout = fileSystem.create(path);
		} else {
			fsout = fileSystem.append(path);
		}

		// wrap the outputstream with a writer
		PrintWriter writer = new PrintWriter(fsout);
		writer.append(content+"\n");
		writer.close();
		fsout.close();
	}
}