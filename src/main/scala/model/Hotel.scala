package model

case class Hotel(
                  Id: String,
                  Name: String,
                  Country: String,
                  City: String,
                  Address: String,
                  Latitude: Option[Double],
                  Longitude: Option[Double]
                ) {

  def hasCoordinates(): Boolean = Latitude.isDefined && Longitude.isDefined

}

case class HotelWithGeohash(
                             id: String,
                             name: String,
                             country: String,
                             city: String,
                             address: String,
                             latitude: Double,
                             longitude: Double,
                             geohash: String
                           )

case class HotelWithWeather(
                             id: String,
                             name: String,
                             country: String,
                             city: String,
                             address: String,
                             latitude: Double,
                             longitude: Double,
                             geohash: String,
                             avgTmprF: Option[Double],
                             avgTmprC: Option[Double],
                             wthrDate: Option[String],
                             wthrYear: Option[String],
                             wthrMonth: Option[String],
                             wthrDay: Option[String]
                           )

