package zio

import zio.json.{ JsonDecoder, JsonEncoder, JsonStreamDelimiter, ast }
import zio.stream._

import java.io.{ File, IOException }
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{ OpenOption, Path, Paths }

trait JsonPackagePlatformSpecific {
  def readJsonAs(file: File): ZStream[Any, Throwable, ast.Json] =
    readJsonLinesAs[ast.Json](file)

  def readJsonAs(path: Path): ZStream[Any, Throwable, ast.Json] =
    readJsonLinesAs[ast.Json](path)

  def readJsonAs(path: String): ZStream[Any, Throwable, ast.Json] =
    readJsonLinesAs[ast.Json](path)

  def readJsonAs(url: URL): ZStream[Any, Throwable, ast.Json] =
    readJsonLinesAs[ast.Json](url)

  def readJsonLinesAs[A: JsonDecoder](file: File): ZStream[Any, Throwable, A] =
    readJsonLinesAs(file.toPath)

  def readJsonLinesAs[A: JsonDecoder](path: Path): ZStream[Any, Throwable, A] =
    fromPath(path)
      .via(
        ZPipeline.utf8Decode >>>
          stringToChars >>>
          JsonDecoder[A].decodeJsonPipeline(JsonStreamDelimiter.Newline)
      )

  def readJsonLinesAs[A: JsonDecoder](path: String): ZStream[Any, Throwable, A] =
    readJsonLinesAs(Paths.get(path))

  def readJsonLinesAs[A: JsonDecoder](url: URL): ZStream[Any, Throwable, A] = {
    val scoped = ZIO
      .fromAutoCloseable(ZIO.attempt(url.openStream()))
      .refineToOrDie[IOException]

    ZStream
      .fromInputStreamScoped(scoped)
      .via(
        ZPipeline.utf8Decode >>>
          stringToChars >>>
          JsonDecoder[A].decodeJsonPipeline(JsonStreamDelimiter.Newline)
      )
  }

  def writeJsonLines[R](file: File, stream: ZStream[R, Throwable, ast.Json]): RIO[R, Unit] =
    writeJsonLinesAs(file, stream)

  def writeJsonLines[R](path: Path, stream: ZStream[R, Throwable, ast.Json]): RIO[R, Unit] =
    writeJsonLinesAs(path, stream)

  def writeJsonLines[R](path: String, stream: ZStream[R, Throwable, ast.Json]): RIO[R, Unit] =
    writeJsonLinesAs(path, stream)

  def writeJsonLinesAs[R, A: JsonEncoder](file: File, stream: ZStream[R, Throwable, A]): RIO[R, Unit] =
    writeJsonLinesAs(file.toPath, stream)

  def writeJsonLinesAs[R, A: JsonEncoder](path: Path, stream: ZStream[R, Throwable, A]): RIO[R, Unit] =
    stream
      .via(
        JsonEncoder[A].encodeJsonLinesPipeline >>>
          charsToUtf8
      )
      .run(sinkFromPath(path))
      .unit

  def writeJsonLinesAs[R, A: JsonEncoder](path: String, stream: ZStream[R, Throwable, A]): RIO[R, Unit] =
    writeJsonLinesAs(Paths.get(path), stream)

  private def stringToChars: ZPipeline[Any, Nothing, String, Char] =
    ZPipeline.mapChunks[String, Char](_.flatMap(_.toCharArray))

  private def charsToUtf8: ZPipeline[Any, Nothing, Char, Byte] =
    ZPipeline.mapChunksZIO[Any, Nothing, Char, Byte] { chunk =>
      ZIO.succeed {
        Chunk.fromArray {
          new String(chunk.toArray).getBytes(StandardCharsets.UTF_8)
        }
      }
    }

  /**
   * Creates a stream of bytes from a file at the specified path.
   */
  final def fromPath(path: => Path, chunkSize: => Int = ZStream.DefaultChunkSize)(implicit
    trace: Trace
  ): ZStream[Any, Throwable, Byte] =
    ZStream.blocking {
      ZStream
        .acquireReleaseWith(ZIO.attempt(FileChannel.open(path)))(chan => ZIO.succeed(chan.close()))
        .flatMap { channel =>
          ZStream.fromZIO(ZIO.succeed(ByteBuffer.allocate(chunkSize))).flatMap { reusableBuffer =>
            ZStream.repeatZIOChunkOption(
              for {
                bytesRead <- ZIO.attempt(channel.read(reusableBuffer)).asSomeError
                _         <- ZIO.fail(None).when(bytesRead == -1)
                chunk <- ZIO.succeed {
                           reusableBuffer.flip()
                           Chunk.fromByteBuffer(reusableBuffer)
                         }
              } yield chunk
            )
          }
        }
    }

  import java.nio.file.StandardOpenOption._
  import java.{ util => ju }

  /**
   * Uses the provided `Path` to create a [[ZSink]] that consumes byte chunks
   * and writes them to the `File`. The sink will yield count of bytes written.
   */
  final def sinkFromPath(
    path: => Path,
    position: => Long = 0L,
    options: => Set[OpenOption] = Set(WRITE, TRUNCATE_EXISTING, CREATE)
  )(implicit
    trace: Trace
  ): ZSink[Any, Throwable, Byte, Byte, Long] = {

    val scopedChannel = ZIO.acquireRelease(
      ZIO
        .attemptBlockingInterrupt(
          FileChannel
            .open(
              path,
              options.foldLeft(
                new ju.HashSet[OpenOption]()
              ) { (acc, op) =>
                acc.add(op)
                acc
              } // for avoiding usage of different Java collection converters for different scala versions
            )
            .position(position)
        )
    )(chan => ZIO.attemptBlocking(chan.close()).orDie)

    ZSink.unwrapScoped {
      scopedChannel.map { chan =>
        ZSink.foldLeftChunksZIO(0L) { (bytesWritten, byteChunk: Chunk[Byte]) =>
          ZIO.attemptBlockingInterrupt {
            val bytes = byteChunk.toArray

            chan.write(ByteBuffer.wrap(bytes))

            bytesWritten + bytes.length.toLong
          }
        }
      }
    }
  }

}
