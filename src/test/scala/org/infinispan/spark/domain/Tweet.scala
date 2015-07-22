package org.infinispan.spark.domain

/**
 * @author gustavonalle
 */
case class Tweet(id: Long, user: String, country: String, retweet: Long, followers: Int, text: String)
