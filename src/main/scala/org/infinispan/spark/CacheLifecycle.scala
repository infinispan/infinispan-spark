package org.infinispan.spark

import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.commons.configuration.XMLStringConfiguration


trait CacheManagementAware {
   def cacheAdmin(): CacheAdmin
}

class CacheManagementException extends RuntimeException

class NonExistentCacheException extends CacheManagementException

class CacheAlreadyExistException extends CacheManagementException

class CacheAdmin(remoteCacheManager: RemoteCacheManager) {

   def clear(cacheName: String): Unit = {
      checkExistence(cacheName)
      remoteCacheManager.getCache(cacheName).clear()
   }

   def exists(cacheName: String): Boolean = {
      val remoteCache = remoteCacheManager.getCache(cacheName)
      remoteCache != null
   }

   def delete(cacheName: String): Unit = {
      checkExistence(cacheName)
      remoteCacheManager.administration().removeCache(cacheName)
   }

   def createFromConfig(cacheName: String, xmlConfig: String): Unit = {
      if (exists(cacheName)) throw new CacheAlreadyExistException
      remoteCacheManager.administration().createCache(cacheName, new XMLStringConfiguration(xmlConfig))
   }

   def createFromTemplate(cacheName: String, template: String): Unit = {
      if (exists(cacheName)) throw new CacheAlreadyExistException
      remoteCacheManager.administration().createCache(cacheName, template)
   }

   private def checkExistence(cacheName: String): Unit = {
      if (!exists(cacheName)) throw new NonExistentCacheException
   }
}
