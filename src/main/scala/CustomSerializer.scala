import org.apache.kafka.common.serialization.Serializer
import java.util
import java.nio.ByteBuffer

import org.apache.kafka.common.errors.SerializationException


class CustomSerializer extends Serializer[Sample] {
  private val encoding = "UTF8"

  @Override   override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}


  @Override def serialize(topic: String, data: Sample): Array[Byte] = {
    var sizeOfName = 0
    var sizeOfDate = 0
    try {
      if (data == null) return null
      var serializedName = data.getName.getBytes(encoding)
      sizeOfName = serializedName.length
      val buf = ByteBuffer.allocate(4 + 4 + sizeOfName + 4 + sizeOfDate)
      buf.putInt(data.getID)
      buf.putInt(sizeOfName)
      buf.put(serializedName)
      buf.putInt(sizeOfDate)
      buf.array
    } catch {
      case e: Exception =>
        throw new SerializationException("Error when serializing Sample to byte[]")
    }
  }

  override def close(): Unit = {}

}
