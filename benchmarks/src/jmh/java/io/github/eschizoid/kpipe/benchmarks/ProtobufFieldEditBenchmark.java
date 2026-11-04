package io.github.eschizoid.kpipe.benchmarks;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufBytes;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.openjdk.jmh.annotations.*;

/// Removes one field from a Protobuf record and sets another, the edit a pipeline makes before it
/// forwards the record to Kafka, two ways: decoding into the [DynamicMessage] that
/// `ProtobufFormat` produces, editing and encoding it again; and [ProtobufBytes], which copies the
/// encoded fields it keeps and never decodes.
///
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='ProtobufFieldEditBenchmark'
/// ```
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ProtobufFieldEditBenchmark {

  private static final int SSN = 4;
  private static final int STATUS = 5;

  private Descriptor descriptor;
  private FieldDescriptor ssnField;
  private FieldDescriptor statusField;
  private byte[] record;
  private UnaryOperator<byte[]> removeSsn;
  private UnaryOperator<byte[]> setStatus;

  @Setup
  public void setup() throws Exception {
    final var message = DescriptorProto.newBuilder()
      .setName("User")
      .addField(field("id", 1, Type.TYPE_INT64))
      .addField(field("name", 2, Type.TYPE_STRING))
      .addField(field("email", 3, Type.TYPE_STRING))
      .addField(field("ssn", SSN, Type.TYPE_STRING))
      .addField(field("status", STATUS, Type.TYPE_STRING))
      .addField(field("active", 6, Type.TYPE_BOOL))
      .addField(field("score", 7, Type.TYPE_INT32))
      .addField(field("city", 8, Type.TYPE_STRING))
      .addField(field("country", 9, Type.TYPE_STRING))
      .addField(field("notes", 10, Type.TYPE_STRING))
      .build();
    final var file = FileDescriptorProto.newBuilder().setName("user.proto").addMessageType(message).build();
    descriptor = FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("User");
    ssnField = descriptor.findFieldByNumber(SSN);
    statusField = descriptor.findFieldByNumber(STATUS);

    record = DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByNumber(1), 1_234_567_890L)
      .setField(descriptor.findFieldByNumber(2), "Jane Doe")
      .setField(descriptor.findFieldByNumber(3), "jane.doe@example.com")
      .setField(ssnField, "123-45-6789")
      .setField(statusField, "received")
      .setField(descriptor.findFieldByNumber(6), true)
      .setField(descriptor.findFieldByNumber(7), 742)
      .setField(descriptor.findFieldByNumber(8), "Chicago")
      .setField(descriptor.findFieldByNumber(9), "United States")
      .setField(
        descriptor.findFieldByNumber(10),
        "Customer since 2019; prefers email contact; enrolled in the loyalty programme and paperless billing."
      )
      .build()
      .toByteArray();

    removeSsn = ProtobufBytes.remove(SSN);
    setStatus = ProtobufBytes.setString(STATUS, "processed");
  }

  private static FieldDescriptorProto field(final String name, final int number, final Type type) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type).build();
  }

  @Benchmark
  public byte[] decodeEditEncode() throws InvalidProtocolBufferException {
    return DynamicMessage.parseFrom(descriptor, record)
      .toBuilder()
      .clearField(ssnField)
      .setField(statusField, "processed")
      .build()
      .toByteArray();
  }

  @Benchmark
  public byte[] editEncodedBytes() {
    return setStatus.apply(removeSsn.apply(record));
  }
}
