/**
 * RxMongo Database client binding for YCSB.
 *
 * Submitted by Reid Spencer on
 *
 */

package com.reactific.ycsb

import rxmongo.bson.BinarySubtype.UserDefinedBinary
import rxmongo.bson._
import rxmongo.bson.BSONCodec.ArrayOfStringCodec
import rxmongo.client.{Database, Client}
import rxmongo.driver.{QueryOptions, Projection}

import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger}

import com.yahoo.ycsb.ByteIterator
import com.yahoo.ycsb.DB

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}


object RxMongo {
  var mongo : Client = null
  var database : Database = null
  final val initCount: AtomicInteger = new AtomicInteger(0)
}

case class BSONBinaryByteIterator(bin: BSONBinary) extends ByteIterator {
  val i = bin.buffer.iterator
  def hasNext : Boolean = i.hasNext

  def bytesLeft() : Long = i.len

  def nextByte() : Byte = i.next()
}

/**
 * MongoDB client for YCSB framework.
 *
 * Properties to set:
 *
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 *
 * @author ypai
 */
class RxMongoClient extends DB {

  var rxmongo : Client = null
  var database : Database = null

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  override def init() : Unit = Try {
    RxMongo.initCount.getAndIncrement
    RxMongo.synchronized {
      if (RxMongo.database != null) {
        database = RxMongo.database
        rxmongo = RxMongo.mongo
      } else {
        // initialize MongoDb driver
        val props : Properties = getProperties
        val url : String = props.getProperty("rxmongo.url", "mongodb://localhost:27017/ycsb?minPoolSize=10&maxPoolSize=100")
        val dbname : String = props.getProperty("rxmongo.db", "ycsb")
        rxmongo = Client(url)
        database = rxmongo.database(dbname)
        System.out.println("rxmongo connection created with " + url)
        RxMongo.mongo = rxmongo
        RxMongo.database = database
      }
    }
  } match {
    case Success(x) ⇒ x
    case Failure(xcptn) ⇒
      System.err.println(s"Could not initialize MongoDB client for Loader: $xcptn")
      xcptn.printStackTrace()
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  override def cleanup() : Unit = Try {
    if (RxMongo.initCount.decrementAndGet() <= 0) {
      RxMongo.mongo.close()
      RxMongo.mongo = null
      RxMongo.database = null
    }
  } match {
    case Success(x) ⇒ x
    case Failure(xcptn) ⇒
      System.err.println(s"Could not close MongoDB client: $xcptn")
      xcptn.printStackTrace()
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  override def delete(table: String, key: String) : Int = Try {
    val coll = database.collection(table)
    val future =  coll.delete(Seq(Delete("_id" $eq key, 1))).map { wr ⇒ if (wr.ok != 0) 0 else 1 }
    Await.result(future, 5.seconds)
  } match {
    case Success(x: Int) ⇒ x
    case Failure(xcptn) ⇒
      println("Failure while deleting key=" + key)
      System.err.println(xcptn.toString)
      1
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  override def insert(table: String, key: String, values: java.util.HashMap[String,ByteIterator]) : Int = Try {
    val coll = database.collection(table)
    val b = BSONBuilder()
    b.string("_id", key)
    val i = values.entrySet.iterator()
    while (i.hasNext) {
      val e = i.next()
      b.binary(e.getKey, e.getValue.toArray, UserDefinedBinary)
    }
    val future = coll.insertOne(b.result).map { wr ⇒ if (wr.ok != 0) 0 else 1}
    Await.result(future, 10.seconds)
  } match {
    case Success(x) ⇒ x
    case Failure(xcptn) ⇒
      println("Failure while inserting key=" + key)
      xcptn.printStackTrace()
      1
  }

  private def makeProjection(fields: java.util.Set[String]) : Option[Projection] = {
    if (fields != null) {
      val field = fields.iterator()
      var proj = Projection()
      while (field.hasNext) {
        val f = field.next
        proj = proj.include(f)
      }
      Some(proj)
    } else {
      None
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  override def read(table: String, key: String, fields: java.util.Set[String],
    result: java.util.HashMap[String, ByteIterator]) : Int = Try {
    val collection = database.collection(table)
    val query = Query("_id" $eq key)
    val future = collection.findOne(query, makeProjection(fields)).map { cursor ⇒
      cursor.next().map { obj ⇒
        val map = obj.toMap.map { case (k, v) ⇒ k -> BSONBinaryByteIterator(v.asInstanceOf[BSONBinary])}
        result.putAll(map.asJava)
      }
      0
    }
    Await.result(future, 5.seconds)
  } match {
    case Success(x) ⇒ x
    case Failure(xcptn) ⇒
      println("Failure while reading key=" + key)
      System.err.println(xcptn.toString)
      1
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  override def update(table: String , key: String, values: java.util.HashMap[String, ByteIterator]) : Int = Try {
    val collection = database.collection(table)
    val q = Query("_id" $eq key)
    val i = values.entrySet.iterator
    val vals = for ((key, value) ← values.asScala.toSeq) yield {key → value.toArray}

    val update = Update(Query("_id" $eq key), $set[Array[Byte],BSONBinary](vals : _*), upsert = false, multi = false, isolated = false)

    val future = collection.updateOne(update) map { wr ⇒ if (wr.ok != 0) 0 else 1 }
    Await.result(future, 5.seconds)
  } match {
    case Success(x) ⇒ x
    case Failure(xcptn) ⇒
      println("Failure while updating key=" + key)
      System.err.println(xcptn.toString)
      1
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startKey The record key of the first record to read.
   * @param recordCount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  override def scan(table: String, startKey: String, recordCount: Int, fields: java.util.Set[String],
                    result: java.util.Vector[java.util.HashMap[String, ByteIterator]]) : Int = Try {
    val collection = database.collection(table)
    val query = Query("_id" $gte startKey)
    val options = QueryOptions(numberToReturn = recordCount)
    val future = collection.find(query, makeProjection(fields), options) map { cursor ⇒
      while (cursor.hasNext) {
        cursor.next().map { obj ⇒
          val map = obj.toMap.map { case (k, v) ⇒ k -> BSONBinaryByteIterator(v.asInstanceOf[BSONBinary])}
          val hMap = new java.util.HashMap[String, ByteIterator]()
          hMap.putAll(map.asJava)
          result.add(hMap)
        }
      }
      0
    }
    Await.result(future, 5.seconds)
  } match {
      case Success(x) ⇒ x
      case Failure(xcptn) ⇒
        println("Failure while scanning from startKey=" + startKey)
        System.err.println(xcptn.toString)
        1
  }
}
