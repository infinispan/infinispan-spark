package org.infinispan.spark.test

import java.net.HttpURLConnection
import java.security.SecureRandom
import java.security.cert.X509Certificate

import javax.net.ssl._
import sttp.client.{HttpURLConnectionBackend, basicRequest, _}

/**
 * A simple REST client to Infinispan
 */
case class InfinispanClient(port: Int = 11222, tls: Boolean) {
   val SkipSSLValidation: HttpURLConnection => Unit = {
      case connection: HttpsURLConnection =>
         val sc = SSLContext.getInstance("TLS")
         sc.init(null, Array(new TrustAllX509TrustManager), new SecureRandom)
         connection.setSSLSocketFactory(sc.getSocketFactory)
         connection.setHostnameVerifier(new HostnameVerifier {
            override def verify(s: String, sslSession: SSLSession) = true
         })
      case _ =>
   }

   implicit val backend = HttpURLConnectionBackend(customizeConnection = SkipSSLValidation)

   val protocol = if (tls) "https" else "http"

   val baseURL = s"$protocol://localhost:$port/rest/v2"

   def shutdown() = {
      val request = basicRequest.get(uri"$baseURL/server/stop")
      val response = request.send()
      val code = response.code.code
      if (code < 200 || code > 204) throw new Exception(s"Failed to stop server, code $code")
   }

   def cacheExists(name: String): Boolean = {
      val request = basicRequest.get(uri"$baseURL/caches/")
      val response = request.send()
      response.body match {
         case Right(x) => ujson.read(x).arr.map(_.str).contains(name)
         case _ => false
      }
   }

   def isHealthy(cacheManager: String, members: Int): Boolean = {
      val request = basicRequest.get(uri"$baseURL/cache-managers/$cacheManager/health/")
      val response = request.send()
      response.body match {
         case Right(x) =>
            val json = ujson.read(x)
            val clusterHealth = json("cluster_health")
            val health = clusterHealth("health_status")
            val nodes = clusterHealth("number_of_nodes")
            health.str == "HEALTHY" && nodes.num == members
         case Left(x) => throw new RuntimeException(x)
      }
   }

   def createCache(cacheName: String, cfg: String) = {
      if (!cacheExists(cacheName)) {
         val response = basicRequest.post(uri"$baseURL/caches/$cacheName")
           .contentType("application/json")
           .body(cfg).send()
         val status = response.code.code
         if (status < 200 || status > 204) throw new RuntimeException(s"Failed to create cache, code $status")
      }
   }

   class TrustAllX509TrustManager extends X509TrustManager {
      def getAcceptedIssuers = new Array[X509Certificate](0)

      def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {}

      def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {}
   }

}
