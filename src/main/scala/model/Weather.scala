package model

case class Weather(
                    lng: Double,
                    lat: Double,
                    avg_tmpr_f: Double,
                    avg_tmpr_c: Double,
                    wthr_date: String,
                    year: String,
                    month: String,
                    day: String
                  )

case class WeatherWithGeohash(
                               lng: Double,
                               lat: Double,
                               avgTmprF: Double,
                               avgTmprC: Double,
                               wthrDate: String,
                               year: String,
                               month: String,
                               day: String,
                               geohash: String
                             )
