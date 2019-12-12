import java.nio.ByteBuffer

import org.apache.kafka.common.serialization.Deserializer
import java.util

import org.apache.kafka.common.errors.SerializationException


class CustomDeserializer extends Deserializer[Sample] {
  private val encoding = "UTF8"

  @Override   override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}


  @Override def deserialize(topic: String, data: Array[Byte]): Sample = try {
    if (data == null) {
      System.out.println("Null recieved at deserialize")
      return null
    }
    val buf = ByteBuffer.wrap(data)
    val id = buf.getInt
    val sizeOfName = buf.getInt
    val nameBytes = new Array[Byte](sizeOfName)
    buf.get(nameBytes)

    val deserializedName = new String(nameBytes, encoding)
    val sizeOfDate = buf.getInt

    new Sample(id, deserializedName)
  } catch {
    case e: Exception =>
      throw new SerializationException("Error when deserializing byte[] to Sample")
  }

  override def close(): Unit = {}

}
