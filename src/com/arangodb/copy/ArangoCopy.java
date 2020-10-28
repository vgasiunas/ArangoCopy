package com.arangodb.copy;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.ArangoDBVersion;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.util.MapBuilder;
import com.arangodb.velocypack.VPackSlice;

public class ArangoCopy {

	private static class ConnSettings {
		public ConnSettings() { }
		public String host = "localhost";
		public int port = 8529;
		public String user = "root";
		public String password = "";
		public boolean useSSL = false;
		public String caCertPath = "";
		private String dbName = "db";
		private String collectionName = "docs";
	};

	// options with their default values
	private static int batchSize = 50;
	private static int numThreads = 1;
	private static ConnSettings srcSettings = new ConnSettings();
	private static ConnSettings dstSettings = new ConnSettings();

	private static class ThreadData {
		public String startKey = "";
		public String endKey = "";
		public long copyTime = 0;
		public long docsCopied = 0;
	};

	private static ThreadData[] threadData;
	private static boolean failed = false;

	private static void parseCommandLineOptions(String[] args) {
		Option option = null;
		Options options = new Options();

		option = new Option("t", "threads", true, "number of threads");
        option.setType(Number.class);
        options.addOption(option);

        option = new Option("b", "batchsize", true, "batch size");
        option.setType(Number.class);
        options.addOption(option);

        option = new Option("sh", "srchost", true, "source host name");
        options.addOption(option);

        option = new Option("sP", "srcport", true, "source port number");
        option.setType(Number.class);
        options.addOption(option);

        option = new Option("su", "srcuser", true, "source user name");
        options.addOption(option);

        option = new Option("sp", "srcpassword", true, "source password");
        options.addOption(option);

        option = new Option("sS", "srcusessl", false, "source use SSL");
        options.addOption(option);

        option = new Option("sC", "srccacert", true, "source path to the CA certificate");
        options.addOption(option);

        option = new Option("sd", "srcdb", true, "source database name");
        options.addOption(option);

        option = new Option("sc", "srccoll", true, "source collection name");
        options.addOption(option);

        option = new Option("dh", "dsthost", true, "destination host name");
        options.addOption(option);

        option = new Option("dP", "dstport", true, "destination port number");
        option.setType(Number.class);
        options.addOption(option);

        option = new Option("du", "dstuser", true, "destination user name");
        options.addOption(option);

        option = new Option("dp", "dstpassword", true, "destination password");
        options.addOption(option);

        option = new Option("dS", "dstusessl", false, "destination use SSL");
        options.addOption(option);

        option = new Option("dC", "dstcacert", true, "destination path to the CA certificate");
        options.addOption(option);

        option = new Option("dd", "dstdb", true, "destination database name");
        options.addOption(option);

        option = new Option("dc", "dstcoll", true, "destination collection name");
        options.addOption(option);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);

	        if (cmd.hasOption("threads")) {
	        	numThreads = ((Number)cmd.getParsedOptionValue("threads")).intValue();
	        }

	        if (cmd.hasOption("batchsize")) {
	        	batchSize = ((Number)cmd.getParsedOptionValue("batchsize")).intValue();
	        }

	        if (cmd.hasOption("srchost")) {
	        	srcSettings.host = cmd.getOptionValue("srchost");
	        }

	        if (cmd.hasOption("srcport")) {
	        	srcSettings.port = ((Number)cmd.getParsedOptionValue("srcport")).intValue();
	        }

	        if (cmd.hasOption("srcuser")) {
	        	srcSettings.user = cmd.getOptionValue("srcuser");
	        }

	        if (cmd.hasOption("srcpassword")) {
	        	srcSettings.password = cmd.getOptionValue("srcpassword");
	        }

	        if (cmd.hasOption("srcsrcusessl")) {
	        	srcSettings.useSSL = true;
	        }

	        if (cmd.hasOption("srccacert")) {
	        	srcSettings.caCertPath = cmd.getOptionValue("scr-cacert");
	        }

	        if (cmd.hasOption("srcdb")) {
	        	srcSettings.dbName = cmd.getOptionValue("srcdb");
	        }

	        if (cmd.hasOption("srccoll")) {
	        	srcSettings.collectionName = cmd.getOptionValue("srccoll");
	        }

	        if (cmd.hasOption("dsthost")) {
	        	dstSettings.host = cmd.getOptionValue("dsthost");
	        }

	        if (cmd.hasOption("dstport")) {
	        	dstSettings.port = ((Number)cmd.getParsedOptionValue("dstport")).intValue();
	        }

	        if (cmd.hasOption("dstuser")) {
	        	dstSettings.user = cmd.getOptionValue("dstuser");
	        }

	        if (cmd.hasOption("dstpassword")) {
	        	dstSettings.password = cmd.getOptionValue("dstpassword");
	        }

	        if (cmd.hasOption("dstusessl")) {
	        	dstSettings.useSSL = true;
	        }

	        if (cmd.hasOption("dstcacert")) {
	        	dstSettings.caCertPath = cmd.getOptionValue("dstcacert");
	        }

	        if (cmd.hasOption("dstdb")) {
	        	dstSettings.dbName = cmd.getOptionValue("dstdb");
	        }

	        if (cmd.hasOption("srccoll")) {
	        	dstSettings.collectionName = cmd.getOptionValue("dstcoll");
	        }

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("arangocopy", options);
            System.exit(1);
        }
	}

	private static void createDatabase(ArangoDB arangoDB) {
		try {
			ArangoDatabase db = arangoDB.db(dstSettings.dbName);
			if (!db.exists()) {
				arangoDB.createDatabase(dstSettings.dbName);
				System.out.println("Database created: " + dstSettings.dbName);
			}
		} catch (ArangoDBException e) {
			System.err.println("Failed to create database: " + dstSettings.dbName + "; " + e.getMessage());
			failed = true;
		}
	}

	private static void recreateCollection(ArangoDB arangoDB) {
		try {
			if (arangoDB.db(dstSettings.dbName).collection(dstSettings.collectionName).exists()) {
				arangoDB.db(dstSettings.dbName).collection(dstSettings.collectionName).drop();
			}
			// TODO: copy source collection properties
			CollectionEntity collEntity = arangoDB.db(dstSettings.dbName).createCollection(dstSettings.collectionName);
			System.out.println("Collection created: " + collEntity.getName());
		} catch (ArangoDBException e) {
			System.err.println("Failed to create collection: " + dstSettings.collectionName + "; " + e.getMessage());
			failed = true;
		}
	}

	private static ArangoDB connectToDB(ConnSettings settings) {
		try {
			ArangoDB.Builder builder = new ArangoDB.Builder();
			builder.host(settings.host, settings.port)
				.user(settings.user)
				.password(settings.password);

			if (settings.useSSL) {
				FileInputStream is = new FileInputStream (settings.caCertPath);
				CertificateFactory cf = CertificateFactory.getInstance("X.509");
				X509Certificate caCert = (X509Certificate) cf.generateCertificate(is);

				TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
				KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
				ks.load(null);
				ks.setCertificateEntry("caCert", caCert);

				tmf.init(ks);

				SSLContext sslContext = SSLContext.getInstance("TLS");
				sslContext.init(null, tmf.getTrustManagers(), null);

				builder.useSsl(settings.useSSL)
					.sslContext(sslContext);
			}

			ArangoDB arangoDB = builder.build();
			ArangoDBVersion version = arangoDB.getVersion();
			System.out.println("Connected to ArangoDB " + version.getVersion());
			return arangoDB;
		}
		catch (Exception e) {
			System.out.println("Error while connecting to the database " + e.getMessage());
			failed = true;
			return null;
		}
	}

	private static void printSummary() {
		long docsCopied = 0;
		for (ThreadData ts : threadData) {
			docsCopied += ts.docsCopied;
		}
		long copyTime = 0;
		for (ThreadData ts : threadData) {
			if (ts.copyTime > copyTime) {
				copyTime = ts.copyTime;
			}
		}
		System.out.println("Total copy time: " + copyTime + "ms");
		System.out.println("Docs copied per second: " + ((long)docsCopied) * 1000 / copyTime);
	}

	private static void partitionWork(ArangoDB srcArangoDB) {
		try {
			ArangoDatabase srcDB = srcArangoDB.db(srcSettings.dbName);
			long totalCnt =
				srcDB.query("RETURN COUNT(@@coll)",
						new MapBuilder().put("@coll", srcSettings.collectionName).get(),
						VPackSlice.class)
				    .next().getAsLong();
			System.out.println(totalCnt + " documents found in collection " + srcSettings.collectionName);
			System.out.println("Partitioning work to threads...");
			long startTime = System.currentTimeMillis();
			if (totalCnt > numThreads) {
				long cntPerThread = totalCnt / numThreads;
				for (int i = 1; i < numThreads; i++) {
					String key =
						srcDB.query("FOR d IN @@coll FILTER d._key >= @startKey SORT d._key LIMIT @cnt, 1 RETURN d._key",
							new MapBuilder()
								.put("@coll", srcSettings.collectionName)
								.put("startKey", threadData[i-1].startKey)
								.put("cnt", cntPerThread).get(),
							VPackSlice.class)
					   .next().getAsString();
					threadData[i-1].endKey = key;
					threadData[i].startKey = key;
				}
			}
			if (totalCnt > 0) {
				threadData[numThreads-1].endKey =
					srcDB.query("FOR d IN @@coll SORT d._key DESC LIMIT 1 RETURN d._key",
						new MapBuilder()
							.put("@coll", srcSettings.collectionName).get(),
						VPackSlice.class)
				   .next().getAsString() + "_";
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Work partitioned in " + (endTime - startTime) + "ms");
		}
		catch (ArangoDBException e) {
			System.out.println("Failed to query source collection: " + e.getMessage());
			failed = true;
		}
	}

	private static void copyData(ArangoDB srcArangoDB, ArangoDB dstArangoDB, int threadIndex) {
		try {
			ArangoDatabase srcDB = srcArangoDB.db(srcSettings.dbName);
			ArangoCollection dstColl = dstArangoDB.db(dstSettings.dbName).collection(dstSettings.collectionName);

			long startTime, endTime = 0;
			startTime = System.currentTimeMillis();
			long docsCopied = 0;

			ArangoCursor<VPackSlice> cursor =
				srcDB.query("FOR d IN @@coll FILTER d._key >= @startKey and d._key < @endKey RETURN d",
						new MapBuilder()
							.put("@coll", srcSettings.collectionName)
							.put("startKey", threadData[threadIndex].startKey)
							.put("endKey", threadData[threadIndex].endKey).get(),
						new AqlQueryOptions().batchSize(batchSize),
						VPackSlice.class);

			ArrayList<VPackSlice> batch = new ArrayList<VPackSlice>();
			while (cursor.hasNext()) {
				VPackSlice doc = cursor.next();
				batch.add(doc);
				if (batch.size() == batchSize) {
					dstColl.insertDocuments(batch);
					batch.clear();
					docsCopied += batchSize;
				}
			}

			if (batch.size() > 0) {
				dstColl.insertDocuments(batch);
				docsCopied += batch.size();
			}

			endTime = System.currentTimeMillis();
			threadData[threadIndex].copyTime = (endTime-startTime);
			threadData[threadIndex].docsCopied = docsCopied;
			System.out.println("Thread " + threadIndex + " copied " + docsCopied + " documents in " + (endTime-startTime) + "ms");
		}
		catch (ArangoDBException e) {
			System.out.println("Error while copying data " + e.getMessage());
			failed = true;
		}
	}

	private static void copyDataMultithreaded(ArangoDB srcArangoDB, ArangoDB dstArangoDB) {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		System.out.println("Copying data...");

		for (int i = 0; i < numThreads; i++) {
			final int threadIndex = i;
			Thread thread = new Thread(() -> {
				copyData(srcArangoDB, dstArangoDB, threadIndex);
			});
			threads.add(thread);
			thread.start();
		}
		try {
			for (Thread thread : threads) {
				thread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		parseCommandLineOptions(args);

		ArangoDB srcArangoDB = connectToDB(srcSettings);
		ArangoDB dstArangoDB = connectToDB(dstSettings);

		threadData = new ThreadData[numThreads];
		for (int i = 0; i < numThreads; i++) {
			threadData[i] = new ThreadData();
		}

		if (!failed) {
			createDatabase(dstArangoDB);
		}
		if (!failed) {
			recreateCollection(dstArangoDB);
		}
		if (!failed) {
			partitionWork(srcArangoDB);
		}
		if (!failed) {
			copyDataMultithreaded(srcArangoDB, dstArangoDB);
		}

		if (srcArangoDB != null) {
			srcArangoDB.shutdown();
		}
		if (dstArangoDB != null) {
			dstArangoDB.shutdown();
		}
		if (!failed) {
			printSummary();
		}
	}

}
