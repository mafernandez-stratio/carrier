/*
 *
 *  * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  * See the NOTICE file distributed with this work for additional information
 *  * regarding copyright ownership.  The STRATIO (C) licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package io.miguel0afd.carrier

import java.util

import org.apache.ignite.cache.CachePeekMode
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.{IgniteCache, IgniteCluster, Ignition, Ignite}
import scala.collection.JavaConversions._

case class Fruit(name: String, origin: String)

object CarrierApp extends App {

  //System.setProperty("IGNITE_QUIET", "false");

  /*
  val config: IgniteConfiguration = new IgniteConfiguration
  val discoverySpi: TcpDiscoverySpi = new TcpDiscoverySpi
  val ipFinder: TcpDiscoveryVmIpFinder = new TcpDiscoveryVmIpFinder
  val addresses: util.HashSet[String] = new util.HashSet[String]
  addresses.add("127.0.0.1:47500..47509")
  ipFinder.setAddresses(addresses)
  discoverySpi.setIpFinder(ipFinder)
  config.setDiscoverySpi(discoverySpi)
  val ignite: Ignite = Ignition.start(config)
  */

  //val ignite: Ignite = Ignition.start("src/resources/ignite-config.xml")
  val ignite: Ignite = Ignition.start("src/resources/ignite-config.xml")
  //ignite.cluster().localNode().attributes().entrySet().foreach(println)
  println("Local Port: " + ignite.cluster().localNode().attributes().get("TcpCommunicationSpi.comm.tcp.port"))

  //val cluster: IgniteCluster = ignite.cluster

  // Obtain instance of cache named "fruits".
  // Note that different caches may have different generics.
  val cache: IgniteCache[String, Fruit] = ignite.getOrCreateCache("fruits")
  if(cache.size(CachePeekMode.ALL) < 1){
    val fruit: Fruit = Fruit("Durian", "Indonesia")
    cache.put(fruit.name, fruit)
    val result: Fruit = cache.get(fruit.name)
    println(result.name + " - " + result.origin)
  } else {
    val fruit: Fruit = Fruit("Mango", "India")
    cache.put(fruit.name, fruit)
    println("Cache size: " + cache.size(CachePeekMode.PRIMARY))
    val result1: Fruit = cache.get("Durian")
    println(result1.name + " - " + result1.origin)
    val result2: Fruit = cache.get("Mango")
    println(result2.name + " - " + result2.origin)
  }

  //ignite.close
}
