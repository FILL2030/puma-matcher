/*
 * Copyright 2019 Institut Laue–Langevin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.ill.puma.sparkmatcher.utils.conf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator

class Spark(appName : String) {

	var _session: SparkSession = null

	var _accumulators: Map[String, LongAccumulator] = Map()

	// Create a Scala Spark Context.
	val _config = new SparkConf()
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.eventLog.enabled", "true")
		.set("spark.history.fs.logDirectory", "/tmp/spark-events")
		.set("spark.kryoserializer.buffer.max", "2047m")
		.set("spark.driver.maxResultSize", "8g")
		.set("spark.executor.memory", "96g")
		.set("spark.memory.offHeap.enabled", "true")
		.set("spark.memory.offHeap.size", "48g")
		.set("spark.driver.memory", "32g")
		.set("spark.driver.cores", "16")
		.set("spark.sql.shuffle.partitions", ProgramConfig.deduplicationPartitionNumber.toString)

	val session = {
		if (_session == null) {
			_session = SparkSession
				.builder()
				.master("local[*]")
				.appName(appName)
				.config(_config)
				.getOrCreate()
		}

		_session
	}

	val context = {
		session.sparkContext
	}

	def accumulator(name: String): LongAccumulator = {
		var accumulator: LongAccumulator = null
		val accumulatorOption: Option[LongAccumulator] = _accumulators.get(name)

		if (accumulatorOption.isEmpty) {
			accumulator = context.longAccumulator(name)
			_accumulators += (name -> accumulator)

		} else {
			accumulator = accumulatorOption.get
		}

		accumulator
	}

	def dataFrameFromDatabase(url: String, queryOrTable: String): DataFrame = {
		// Database options
		val opts = Map(
			"url" -> url,
			"user" -> ProgramConfig.username,
			"password" -> ProgramConfig.password,
			"driver" -> ProgramConfig.driver,
			"dbtable" -> queryOrTable)

		// Get persons DataFrame
		val dataFrame = _session.
			read.
			format("jdbc").
			options(opts).
			load()

		dataFrame
	}

}
