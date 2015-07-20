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

import java.lang.Iterable
import java.util
import javax.cache.Cache.Entry

import org.apache.ignite.cache.store.CacheStoreAdapter
import org.apache.ignite.lang.IgniteBiInClosure

import scala.collection.JavaConversions._
import scala.collection.mutable

class FakePersistence extends CacheStoreAdapter[String, Fruit] {

  val fakeDatabase: mutable.Map[String, Fruit] = mutable.Map()

  override def delete(key: scala.Any): Unit = {
    println("Deleting entry with key: " + key)
    fakeDatabase.remove(key.asInstanceOf[String])
  }

  override def write(entry: Entry[_ <: String, _ <: Fruit]): Unit = {
    println("Writing entry with key: " + entry.getKey)
    fakeDatabase.put(entry.getKey, entry.getValue)
  }

  override def load(key: String): Fruit = {
    println("Loading entry with key: " + key)
    fakeDatabase.get(key.asInstanceOf[String]).get
  }

  override def deleteAll(keys: util.Collection[_]): Unit = {
    println("Deleting several entries")
    keys.foreach(_ => fakeDatabase.remove(_))
  }

  override def loadCache(clo: IgniteBiInClosure[String, Fruit], args: AnyRef*): Unit = {
    println("Loading cache")
    val firstFruit: Fruit = Fruit("Peach", "Calanda")
    clo.apply(firstFruit.name, firstFruit)
    val secondFruit: Fruit = Fruit("Pear", "Rincon de Soto")
    clo.apply(secondFruit.name, secondFruit)
    val thirdFruit: Fruit = Fruit("Orange", "Valencia")
    clo.apply(thirdFruit.name, thirdFruit)
  }

  override def loadAll(keys: Iterable[_ <: String]): util.Map[String, Fruit] = {
    println("Loading several entries")
    val result: util.Map[String, Fruit] = new util.HashMap[String, Fruit]
    keys.foreach(e => result.put(e, fakeDatabase.get(e).get))
    result
  }

  override def writeAll(entries: util.Collection[Entry[_ <: String, _ <: Fruit]]): Unit = {
    println("Writing several entries")
    entries.foreach(e => fakeDatabase.put(e.getKey, e.getValue))
  }

}
