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
package eu.ill.puma.sparkmatcher.matching.analyser

import java.io.{ByteArrayOutputStream, File}
import java.nio.file.{Files, Paths}
import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntitiesDfType, EntityType, PictureHashDfType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import javax.imageio.ImageIO
import org.apache.commons.exec.{CommandLine, DefaultExecutor, PumpStreamHandler}
import org.apache.commons.io.FileUtils
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel

class PictureHashAnalyser2(
                            val pictureHashDataSource: DataSource,
                            var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                            var dbProperties: Properties = ProgramConfig.dbProperties) extends Analyser {

  val outputEncoder = RowEncoder.apply(StructType(
    Array(
      StructField("document_version_id", LongType, nullable = false),
      StructField("entity_id", LongType, nullable = false),
      StructField("file_path", StringType, nullable = false),
      StructField("width", LongType),
      StructField("height", LongType),
      StructField("hash", StringType)
    )))
  val script = "#! /usr/bin/env python\n#\n# Perceptual image hash calculation tool based on algorithm descibed in\n# Block Mean Value Based Image Perceptual Hashing by Bian Yang, Fan Gu and Xiamu Niu\n#\n# Copyright 2014 Commons Machinery http://commonsmachinery.se/\n# Distributed under an MIT license, please see LICENSE in the top dir.\n\nimport math\nimport argparse\nimport PIL.Image as Image\n\ndef median(data):\n    data = sorted(data)\n    length = len(data)\n    if length % 2 == 0:\n        return (data[length // 2 - 1] + data[length // 2]) / 2.0\n    return data[length // 2]\n\ndef total_value_rgba(im, data, x, y):\n    r, g, b, a = data[y * im.size[0] + x]\n    if a == 0:\n        return 765\n    else:\n        return r + g + b\n\ndef total_value_rgb(im, data, x, y):\n    r, g, b = data[y * im.size[0] + x]\n    return r + g + b\n\ndef translate_blocks_to_bits(blocks, pixels_per_block):\n    half_block_value = pixels_per_block * 256 * 3 / 2\n\n    # Compare medians across four horizontal bands\n    bandsize = len(blocks) // 4\n    for i in range(4):\n        m = median(blocks[i * bandsize : (i + 1) * bandsize])\n        for j in range(i * bandsize, (i + 1) * bandsize):\n            v = blocks[j]\n\n            # Output a 1 if the block is brighter than the median.\n            # With images dominated by black or white, the median may\n            # end up being 0 or the max value, and thus having a lot\n            # of blocks of value equal to the median.  To avoid\n            # generating hashes of all zeros or ones, in that case output\n            # 0 if the median is in the lower value space, 1 otherwise\n            blocks[j] = int(v > m or (abs(v - m) < 1 and m > half_block_value))\n\n\ndef bits_to_hexhash(bits):\n    return '{0:0={width}x}'.format(int(''.join([str(x) for x in bits]), 2), width = len(bits) // 4)\n\n\ndef blockhash_even(im, bits):\n    if im.mode == 'RGBA':\n        total_value = total_value_rgba\n    elif im.mode == 'RGB':\n        total_value = total_value_rgb\n    else:\n        raise RuntimeError('Unsupported image mode: {}'.format(im.mode))\n\n    data = im.getdata()\n    width, height = im.size\n    blocksize_x = width // bits\n    blocksize_y = height // bits\n\n    result = []\n\n    for y in range(bits):\n        for x in range(bits):\n            value = 0\n\n            for iy in range(blocksize_y):\n                for ix in range(blocksize_x):\n                    cx = x * blocksize_x + ix\n                    cy = y * blocksize_y + iy\n                    value += total_value(im, data, cx, cy)\n\n            result.append(value)\n\n    translate_blocks_to_bits(result, blocksize_x * blocksize_y)\n    return bits_to_hexhash(result)\n\ndef blockhash(im, bits):\n    if im.mode == 'RGBA':\n        total_value = total_value_rgba\n    elif im.mode == 'RGB':\n        total_value = total_value_rgb\n    else:\n        raise RuntimeError('Unsupported image mode: {}'.format(im.mode))\n\n    data = im.getdata()\n    width, height = im.size\n\n    even_x = width % bits == 0\n    even_y = height % bits == 0\n\n    if even_x and even_y:\n        return blockhash_even(im, bits)\n\n    blocks = [[0 for col in range(bits)] for row in range(bits)]\n\n    block_width = float(width) / bits\n    block_height = float(height) / bits\n\n    for y in range(height):\n        if even_y:\n            # don't bother dividing y, if the size evenly divides by bits\n            block_top = block_bottom = int(y // block_height)\n            weight_top, weight_bottom = 1, 0\n        else:\n            y_frac, y_int = math.modf((y + 1) % block_height)\n\n            weight_top = (1 - y_frac)\n            weight_bottom = (y_frac)\n\n            # y_int will be 0 on bottom/right borders and on block boundaries\n            if y_int > 0 or (y + 1) == height:\n                block_top = block_bottom = int(y // block_height)\n            else:\n                block_top = int(y // block_height)\n                block_bottom = int(-(-y // block_height)) # int(math.ceil(float(y) / block_height))\n\n        for x in range(width):\n            value = total_value(im, data, x, y)\n\n            if even_x:\n                # don't bother dividing x, if the size evenly divides by bits\n                block_left = block_right = int(x // block_width)\n                weight_left, weight_right = 1, 0\n            else:\n                x_frac, x_int = math.modf((x + 1) % block_width)\n\n                weight_left = (1 - x_frac)\n                weight_right = (x_frac)\n\n                # x_int will be 0 on bottom/right borders and on block boundaries\n                if x_int > 0 or (x + 1) == width:\n                    block_left = block_right = int(x // block_width)\n                else:\n                    block_left = int(x // block_width)\n                    block_right = int(-(-x // block_width)) # int(math.ceil(float(x) / block_width))\n\n            # add weighted pixel value to relevant blocks\n            blocks[block_top][block_left] += value * weight_top * weight_left\n            blocks[block_top][block_right] += value * weight_top * weight_right\n            blocks[block_bottom][block_left] += value * weight_bottom * weight_left\n            blocks[block_bottom][block_right] += value * weight_bottom * weight_right\n\n    result = [blocks[row][col] for row in range(bits) for col in range(bits)]\n\n    translate_blocks_to_bits(result, block_width * block_height)\n    return bits_to_hexhash(result)\n\nif __name__ == '__main__':\n    parser = argparse.ArgumentParser()\n\n    parser.add_argument('--quick', type=bool, default=False,\n        help='Use quick hashing method. Default: False')\n    parser.add_argument('--bits', type=int, default=16,\n        help='Create hash of size N^2 bits. Default: 16')\n    parser.add_argument('--size',\n        help='Resize image to specified size before hashing, e.g. 256x256')\n    parser.add_argument('--interpolation', type=int, default=1, choices=[1, 2, 3, 4],\n        help='Interpolation method: 1 - nearest neightbor, 2 - bilinear, 3 - bicubic, 4 - antialias. Default: 1')\n    parser.add_argument('--debug', action='store_true',\n        help='Print hashes as 2D maps (for debugging)')\n    parser.add_argument('filenames', nargs='+')\n\n    args = parser.parse_args()\n\n    if args.interpolation == 1:\n        interpolation = Image.NEAREST\n    elif args.interpolation == 2:\n        interpolation = Image.BILINEAR\n    elif args.interpolation == 3:\n        interpolation = Image.BICUBIC\n    elif args.interpolation == 4:\n        interpolation = Image.ANTIALIAS\n\n    if args.quick:\n        method = blockhash_even\n    else:\n        method = blockhash\n\n    for fn in args.filenames:\n        im = Image.open(fn)\n\n        # convert indexed/grayscale images to RGB\n        if im.mode == '1' or im.mode == 'L' or im.mode == 'P':\n            im = im.convert('RGB')\n        elif im.mode == 'LA':\n            im = im.convert('RGBA')\n\n        if args.size:\n            size = args.size.split('x')\n            size = (int(size[0]), int(size[1]))\n            im = im.resize(size, interpolation)\n\n        hash = method(im, args.bits)\n\n        print('{hash}  {fn}'.format(fn=fn, hash=hash))\n\n        if args.debug:\n            bin_hash = '{:0{width}b}'.format(int(hash, 16), width=args.bits ** 2)\n            map = [bin_hash[i:i+args.bits] for i in range(0, len(bin_hash), args.bits)]\n            print(\"\")\n            print(\"\\n\".join(map))\n            print(\"\")"

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    val hashFromDb = pictureHashDataSource.loadData._2
    val pictureDF = input.head._2

    //hash picture
    val analysed = pictureDF
      .join(hashFromDb, Seq("document_version_id", "entity_id"), "leftanti")
      .map(hashMapFunction, outputEncoder)
      .cache()

    //save result
    analysed.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "picture_hash", dbProperties)

    //merge analysed and hashFromDb
    val result = hashFromDb.drop("id") union analysed

    //analysis
    (PictureHashDfType, result)
  }

  def hashMapFunction = new MapFunction[Row, Row]() {
    override def call(row: Row): Row = {

      val documentId = row.getLong(row.fieldIndex("document_version_id"))
      val entityId = row.getLong(row.fieldIndex("entity_id"))
      val path = row.getString(row.fieldIndex("file_path"))

      try {

        val img = ImageIO.read(new File(path))
        val width = img.getWidth.toLong
        val height = img.getHeight.toLong

        val comand = buildCommand(path)
        val response = executeCommand(comand)
        val hash = parseResponse(response)

        Row.fromTuple((documentId, entityId, path, width, height, hash))
      } catch {
        case e: Exception => {
          e.printStackTrace
          Row.fromTuple((documentId, entityId, path, null, null, null))
        }
      }
    }
  }

  def buildCommand(path: String): String = {

    //put script
    if (Files.notExists(Paths.get("/tmp/blockhash.py"))) {
      FileUtils.writeStringToFile(new File("/tmp/blockhash.py"), script, "UTF-8")
    }

    //build command
    val command = "python /tmp/blockhash.py " + path

    //return
    command
  }

  def executeCommand(command: String): String = {
    val outputStream = new ByteArrayOutputStream()
    val commandline = CommandLine.parse(command)
    val exec = new DefaultExecutor()
    val streamHandler = new PumpStreamHandler(outputStream)
    exec.setStreamHandler(streamHandler)
    exec.execute(commandline)

    outputStream.toString()
  }

  def parseResponse(response: String): String = {
    response.split("\\W+").filter(_.length == 64).head
  }

  override def name = "picture analyser"

  override def validInputType: List[DataFrameType] = EntitiesDfType :: Nil

  override def maxInputNumber: Int = 1

}
