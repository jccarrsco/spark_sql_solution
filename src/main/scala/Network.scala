import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object Network{

  //Extract the social network of a given sender
  def userNetwork(user: String, sqlContext: SQLContext, emails: DataFrame) = {

    // get email communication for user
		// Let us make use of the original data set and extract only "sender" and "to" running SQL queries programmatically
		// If we want to run queries programmatically, we need to register the DataFrame as a TempTable
		emails.registerTempTable("emails")
		val fromToSQL = sqlContext.sql("SELECT sender, to FROM emails")

		// fromToSQL is returned as a DataFrame
		// register as Temp Table so we execute SQL queries
		fromToSQL.registerTempTable("fromTo")
		// obtain emails sent by user
		val userEmails = sqlContext.sql("SELECT * FROM fromTo WHERE sender="+user)
		//userEmails.show()
		userEmails.count()

		// Convert DataFrame to RDD so we can manipualate using RDD operations
		val userEmailsRDD = userEmails.rdd
		userEmailsRDD.count()		// get number of emails sent by user
		//userEmailsRDD.take(2).foreach(println)

		// Extract receipients
		import org.apache.spark.sql.Row	// df.rdd returns an RDD of "Row"

		// extract only receipients
		val receipients = userEmailsRDD.map{case Row(from, to) => to}

		// obtain a flat list of unique receipietns with the number of emails sent
		//import scala.mutable.WrappedArray
		val uniqueReceipients = receipients.flatMap( r => r.asInstanceOf[Seq[String]])
																			.map((_,1))
																			.reduceByKey(_+_)

		//  Create tuples of the form (sender, receiver, count)
		val senderReceiverCount = uniqueReceipients.map{case (r, c) => (user, r, c)}

		// Prepare to send edges to a file of the format "sender receiver count"
		// replace quotes by empty string
		//val sRCReady = senderReceiverCount.map{case (s, r, c) => s.replace("\"","") + " " + r + " " + c}

    // unregister temp tables
    sqlContext.dropTempTable("emails")
    sqlContext.dropTempTable("fromTo")
    // return RDD of (user, receiver, count)
    senderReceiverCount
  }

  //Extract the social network of all the nodes at once
  //import org.apache.spark.rdd.RDD
  def wholeNetwork(users: Iterable[String], sqlContext: SQLContext, emails: DataFrame) = {
    // Iterate each user and pass it to the "userNetwork" function. Make sure you escape string users
    val edges = users.map{s => userNetwork("\"" + s + "\"", sqlContext, emails)}

    val reducedEdges = edges.reduce(_ union _)

    // return reducedEdges
    reducedEdges
  }

  // Construct the Pajek network so that it can be analyzed
  import scala.io.Source
  import java.io.FileWriter
  def createPajekNetwork(nodesFile: String, edgesFile: String, netFile: String) {
    // get the nodes and count the number of nodes
    val nodes = Source.fromFile(nodesFile).getLines().toList
    val numNodes = nodes.size

    // get the edges
    val edges = Source.fromFile(edgesFile).getLines().toList

    println("printing lines from edges")
    edges.take(3).foreach(println)

    // create a file with ".net" extension
    val fw = new FileWriter(netFile, true)

    try{
      fw.write("*Vertices " + numNodes + "\n")

      // send the nodes to the file
      nodes.map(l => if(l.trim != "") fw.write(l + "\n"))

      // Now send the edges to the file
      fw.write("*arcs \n")
      edges.map(l => if(l.trim != "") fw.write(l + "\n"))
    } finally{
      fw.close()
    }
  }

}
