import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {

  //source: https://www.movable-type.co.uk/scripts/latlong.html
  val haversine = (lat1 : Double, lon1 : Double, lat2 : Double, lon2 : Double) => {
    // convert lat&lon to radians
    val rad = 6371000.0; //6371 km is earth's radius
    val diff_Lat = (lat2 - lat1);
    val diff_Lon = (lon2 - lon1);
    val a = Math.pow(Math.sin(diff_Lat / 2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(diff_Lon / 2), 2);
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    val d = rad * c;
    d
  }

  val eight_hours = 8*60*60; //8 hours as seconds

  /*the expected query:
    select *
    from
    tripsProvided a,
    tripsProvided b
    where
    distance (a. dropofflocation , b. pickuplocation ) < r and
    distance (b. dropofflocation , a. pickuplocation ) < r and
    a. dropofftime < b. pickuptime and
    a. dropofftime + 8 hours > b. pickuptime
    */

  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._
    //get necessary columns.
   // val get_data = trips.select("tpep_pickup_datetime","tpep_dropoff_datetime","pickup_longitude","pickup_latitude","dropoff_longitude", "dropoff_latitude");
    
    //replace columns with appropriate forms
    val real_data = trips.withColumn("tpep_pickup_datetime",col("tpep_pickup_datetime").cast("double")).
    withColumn("tpep_dropoff_datetime",col("tpep_dropoff_datetime").cast("double")).
    withColumn("pickup_longitude", toRadians(col("pickup_longitude"))).
    withColumn("pickup_latitude", toRadians(col("pickup_latitude"))).
    withColumn("dropoff_longitude", toRadians(col("dropoff_longitude"))).
    withColumn("dropoff_latitude", toRadians(col("dropoff_latitude")));
    
    def calc_dist = udf(haversine) //distance formula

    //need to separate the problems.
    val dist_diff = dist / (6371.0*1000.0)

    //simplify the time by dividing to 8 hours, for faster calculation, create buckets.
    //in order to decide within 8 hours or not, divide the values to 8 hours in seconds and then check if it is less than or equal to 1.
    //similar in deciding the within r meters. use mod.
    val chosen_data1 = real_data.withColumn("pickup_time",floor($"tpep_pickup_datetime"/ eight_hours)).
    withColumn("dropoff_time",floor($"tpep_dropoff_datetime" / eight_hours)).
    withColumn("pickup_lat",floor($"pickup_latitude"/ dist_diff)).
    withColumn("dropoff_lat",floor($"dropoff_latitude"/ dist_diff));

    //create dataset with rows of neighboring buckets
    val chosen_data2 = chosen_data1.
    withColumn("dropoff_time", explode(array($"dropoff_time", $"dropoff_time"+1))).
    withColumn("pickup_lat", explode(array($"pickup_lat"-1, $"pickup_lat", $"pickup_lat"+1))).
    withColumn("dropoff_lat", explode(array($"dropoff_lat"-1, $"dropoff_lat", $"dropoff_lat"+1)))

    //join the buckets
    val join_data = chosen_data2.as("a").join(chosen_data1.as("b"), ($"a.pickup_lat" === $"b.dropoff_lat") && ($"a.dropoff_lat"=== $"b.pickup_lat") && ($"a.dropoff_time" === $"b.pickup_time"));

    //filter the data to get the result
    val result_trips = join_data.filter(calc_dist($"a.dropoff_latitude", $"a.dropoff_longitude", $"b.pickup_latitude", $"b.pickup_longitude") < dist
    && calc_dist($"b.dropoff_latitude", $"b.dropoff_longitude", $"a.pickup_latitude", $"a.pickup_longitude") < dist
    && ($"a.tpep_dropoff_datetime" < $"b.tpep_pickup_datetime") && $"a.tpep_dropoff_datetime" + eight_hours > $"b.tpep_pickup_datetime") 

    //3107 correct result for test.
    result_trips

  }
}
