package client

import com.opencagedata.geocoder.OpenCageClient
import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class GeocodeSyncClient(num: Int) extends AutoCloseable {

  private val client = new OpenCageClient(authKey = "dc0aa92163b348d1a9f4eff199af842d", backend = AsyncHttpClientFutureBackend())


  def forwardGeocode(address: String): Option[(Double, Double)] = {
    val response = Await.result(client.forwardGeocode(address), 5 seconds)
    response.results.headOption.flatMap(r => r.geometry.map(g => (g.lat, g.lng)))
  }

  def close(): Unit = client.close()

  def getNum(): Int = num

}

object GeocodeSyncClient{

  OpenCageClient.defaultBackend.close()

  var counter = new AtomicInteger()

  def apply(): GeocodeSyncClient = {
    new GeocodeSyncClient(counter.incrementAndGet())
  }

}
