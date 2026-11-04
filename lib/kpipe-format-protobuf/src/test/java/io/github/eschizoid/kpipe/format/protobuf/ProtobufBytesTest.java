package io.github.eschizoid.kpipe.format.protobuf;

import static io.github.eschizoid.kpipe.format.protobuf.ProtobufBytes.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ProtobufBytesTest {

  private static final int ID = 1;
  private static final int NAME = 2;
  private static final int SSN = 3;
  private static final int SCORES = 4;
  private static final int ACTIVE = 5;

  private static Descriptor descriptor;

  @BeforeAll
  static void buildDescriptor() throws Exception {
    final var message = DescriptorProto.newBuilder()
      .setName("User")
      .addField(field("id", ID, Type.TYPE_INT64))
      .addField(field("name", NAME, Type.TYPE_STRING))
      .addField(field("ssn", SSN, Type.TYPE_STRING))
      .addField(
        field("scores", SCORES, Type.TYPE_INT32)
          .toBuilder()
          .setLabel(Label.LABEL_REPEATED)
          .setOptions(FieldOptions.newBuilder().setPacked(true))
          .build()
      )
      .addField(field("active", ACTIVE, Type.TYPE_BOOL))
      .build();
    final var file = FileDescriptorProto.newBuilder()
      .setName("user.proto")
      .setSyntax("proto3")
      .addMessageType(message)
      .build();
    descriptor = FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("User");
  }

  private static FieldDescriptorProto field(final String name, final int number, final Type type) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type).build();
  }

  private static DynamicMessage.Builder user() {
    return DynamicMessage.newBuilder(descriptor)
      .setField(descriptor.findFieldByNumber(ID), 42L)
      .setField(descriptor.findFieldByNumber(NAME), "Jane")
      .setField(descriptor.findFieldByNumber(SSN), "123-45-6789")
      .addRepeatedField(descriptor.findFieldByNumber(SCORES), 7)
      .addRepeatedField(descriptor.findFieldByNumber(SCORES), 9);
  }

  private static DynamicMessage decode(final byte[] bytes) throws Exception {
    return DynamicMessage.parseFrom(descriptor, bytes);
  }

  @Test
  void removeDropsTheFieldAndKeepsTheRest() throws Exception {
    final var edited = decode(remove(SSN).apply(user().build().toByteArray()));

    assertEquals(user().clearField(descriptor.findFieldByNumber(SSN)).build(), edited);
  }

  @Test
  void removeDropsAPackedRepeatedField() throws Exception {
    final var edited = decode(remove(SCORES).apply(user().build().toByteArray()));

    assertEquals(0, edited.getRepeatedFieldCount(descriptor.findFieldByNumber(SCORES)));
  }

  @Test
  void removingAnAbsentFieldReturnsTheInputUnchanged() {
    final var bytes = user().build().toByteArray();

    assertSame(bytes, remove(ACTIVE).apply(bytes));
  }

  @Test
  void keepDropsEveryOtherField() throws Exception {
    final var edited = decode(keep(ID, NAME).apply(user().build().toByteArray()));

    assertEquals(
      DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByNumber(ID), 42L)
        .setField(descriptor.findFieldByNumber(NAME), "Jane")
        .build(),
      edited
    );
  }

  @Test
  void setStringReplacesAnExistingValue() throws Exception {
    final var edited = decode(setString(NAME, "Joan").apply(user().build().toByteArray()));

    assertEquals("Joan", edited.getField(descriptor.findFieldByNumber(NAME)));
  }

  @Test
  void setStringAddsAMissingField() throws Exception {
    final var bytes = user().clearField(descriptor.findFieldByNumber(NAME)).build().toByteArray();

    assertEquals("Joan", decode(setString(NAME, "Joan").apply(bytes)).getField(descriptor.findFieldByNumber(NAME)));
  }

  @Test
  void setLongWritesAVarintField() throws Exception {
    final var edited = decode(setLong(ID, 7L).andThen(setLong(ACTIVE, 1)).apply(user().build().toByteArray()));

    assertEquals(7L, edited.getField(descriptor.findFieldByNumber(ID)));
    assertEquals(true, edited.getField(descriptor.findFieldByNumber(ACTIVE)));
  }

  @Test
  void setBytesWritesALengthDelimitedField() throws Exception {
    final var edited = decode(setBytes(NAME, "Joan".getBytes()).apply(user().build().toByteArray()));

    assertEquals("Joan", edited.getField(descriptor.findFieldByNumber(NAME)));
  }

  @Test
  void aSetterThatMeetsAnotherWireTypeFailsTheRecord() {
    final var bytes = user().build().toByteArray();

    final var thrown = assertThrows(IllegalArgumentException.class, () -> setLong(NAME, 1).apply(bytes));
    assertTrue(thrown.getMessage().contains("wire type"), thrown.getMessage());
  }

  @Test
  void anEditMatchesDecodingEditingAndEncodingAgain() throws Exception {
    final var bytes = user().build().toByteArray();
    final var expected = decode(bytes)
      .toBuilder()
      .clearField(descriptor.findFieldByNumber(SSN))
      .setField(descriptor.findFieldByNumber(NAME), "Joan")
      .build();

    assertEquals(expected, decode(setString(NAME, "Joan").apply(remove(SSN).apply(bytes))));
  }

  @Test
  void hasMatchesAFieldThatIsWritten() {
    final var bytes = user().build().toByteArray();

    assertTrue(has(SSN).test(bytes));
    assertFalse(has(ACTIVE).test(bytes), "a proto3 false is not written");
  }

  @Test
  void malformedBytesFailTheRecord() {
    // Field 2, length-delimited, claiming 16 bytes but carrying one.
    final var bytes = new byte[] { 0x12, 0x10, 'J' };

    assertThrows(IllegalArgumentException.class, () -> remove(SSN).apply(bytes));
    assertThrows(IllegalArgumentException.class, () -> has(SSN).test(bytes));
  }

  @Test
  void argumentsThatCannotWorkAreRejectedWhenTheOperatorIsCreated() {
    assertThrows(IllegalArgumentException.class, ProtobufBytes::remove);
    assertThrows(IllegalArgumentException.class, ProtobufBytes::keep);
    assertThrows(IllegalArgumentException.class, () -> remove(0));
    assertThrows(IllegalArgumentException.class, () -> setString(-1, "x"));
    assertThrows(IllegalArgumentException.class, () -> setString(1 << 29, "x"), "the tag would overflow");
    assertThrows(IllegalArgumentException.class, () -> remove(Integer.MAX_VALUE));
    assertDoesNotThrow(() -> setString((1 << 29) - 1, "x"), "the largest field number is legal");
  }
}
