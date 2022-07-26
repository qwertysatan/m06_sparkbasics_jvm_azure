import HotelWeatherJob.{assembleAddress, geocodeHotel, getHotelCoordinates, toHotelWithGeohash, toHotelWithWeather, toWeatherWithGeohash}
import ch.hsr.geohash.GeoHash
import client.GeocodeSyncClient
import model.{Hotel, HotelWithGeohash, HotelWithWeather, Weather, WeatherWithGeohash}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import utils.SparkUtils
import utils.ConfigUtils.conf

class HotelWeatherJob(spark: SparkSession) {

  import spark.implicits._

  val HOTELS_INPUT_PATH = conf.getString("homework.data.input.hotels.url")
  val WEATHER_INPUT_PATH = conf.getString("homework.data.input.weather.url")
  val HOTELS_ENRICHED_WITH_WEATHER_OUTPUT_PATH = conf.getString("homework.data.output.hotels-enriched.url")

  val hotels = spark
    .read
    .schema(ScalaReflection.schemaFor[Hotel].dataType.asInstanceOf[StructType])
    .option("header", "true")
    .csv(HOTELS_INPUT_PATH)
    .as[Hotel]
    .mapPartitions { partition =>
      implicit val geoClient = GeocodeSyncClient()
      val res = partition
        .map(geocodeHotel)
        .filter(_.hasCoordinates)
        .map(toHotelWithGeohash)
        .toList
      if (partition.isEmpty) geoClient.close()
      res.iterator
    }

  val weather = spark
    .read
    .option("header", "true")
    .parquet(WEATHER_INPUT_PATH)
    .as[Weather]
    .map(toWeatherWithGeohash)
    .withColumn("row_num",
      row_number.over(Window.partitionBy($"geohash", $"wthrDate").orderBy($"avgTmprF", $"avgTmprC")))
    .where($"row_num" === 1)
    .drop("row_num")
    .as[WeatherWithGeohash]


  hotels.as("hh")
    .joinWith(weather.as("wh"), $"hh.geohash" === $"wh.geohash", "left")
    .map { case (hotel, weather) => toHotelWithWeather(hotel, Option(weather)) }
    .coalesce(Some(spark.sparkContext.getExecutorMemoryStatus.size - 1).filter(_ != 0).getOrElse(1))
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("wthrYear", "wthrMonth", "wthrDay")
    .parquet(HOTELS_ENRICHED_WITH_WEATHER_OUTPUT_PATH)
}

object HotelWeatherJob {

  def apply(spark: SparkSession): HotelWeatherJob = new HotelWeatherJob(spark)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtils.initSpark()
    HotelWeatherJob(spark)
    spark.stop()
  }

  def assembleAddress(hotel: Hotel): String = s"${hotel.Address}, ${hotel.City}, ${hotel.Country}"

  def toWeatherWithGeohash(w: Weather): WeatherWithGeohash = WeatherWithGeohash(
    w.lng,
    w.lat,
    w.avg_tmpr_f,
    w.avg_tmpr_c,
    w.wthr_date,
    w.year,
    w.month,
    w.day,
    GeoHash.geoHashStringWithCharacterPrecision(w.lat, w.lng, 4)
  )

  def toHotelWithGeohash(h: Hotel): HotelWithGeohash = try {
    HotelWithGeohash(
      h.Id,
      h.Name,
      h.Country,
      h.City,
      h.Address,
      h.Latitude.get,
      h.Longitude.get,
      GeoHash.geoHashStringWithCharacterPrecision(h.Latitude.get, h.Longitude.get, 4)
    )
  } catch {
    case e => {
      throw e
    }
  }

  def toHotelWithWeather(h: HotelWithGeohash, w: Option[WeatherWithGeohash]): HotelWithWeather = HotelWithWeather(
    h.id,
    h.name,
    h.country,
    h.city,
    h.address,
    h.latitude,
    h.longitude,
    h.geohash,
    w.map(_.avgTmprF),
    w.map(_.avgTmprC),
    w.map(_.wthrDate),
    w.map(_.year),
    w.map(_.month),
    w.map(_.day)
  )

  def getHotelCoordinates(hotel: Hotel)(implicit geoClient: GeocodeSyncClient): Option[(Double, Double)] =
    if (!hotel.hasCoordinates())
      geoClient.forwardGeocode(assembleAddress(hotel))
    else
      Some(hotel.Latitude.get, hotel.Longitude.get)

  def geocodeHotel(hotel: Hotel)(implicit geoClient: GeocodeSyncClient): Hotel = {
    if (hotel.Id == 1975684956168L) {
      println(hotel)
      System.exit(1)
    }
    if (!hotel.hasCoordinates()) {
      val coordinates = geoClient.forwardGeocode(assembleAddress(hotel))
      if (coordinates.isDefined) {
        return hotel.copy(Latitude = coordinates.map(_._1), Longitude = coordinates.map(_._2))
      }
    }
    hotel
  }

}

