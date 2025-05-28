package edu.uci.ics.amber.core.tuple

import org.scalatest.funspec.AnyFunSpec
import java.sql.Timestamp
import java.util.List

class AttributeTypeSpec extends AnyFunSpec {

  describe("AttributeType.getAttributeType") {

    it("should return STRING for String class when isLargeBinary is false") {
      assert(
        AttributeType.getAttributeType(
          classOf[String],
          false
        ) === AttributeType.STRING
      )
    }

    it("should return LARGE_BINARY for String class when isLargeBinary is true") {
      assert(
        AttributeType.getAttributeType(
          classOf[String],
          true
        ) === AttributeType.LARGE_BINARY
      )
    }

    it("should return INTEGER for Integer class") {
      assert(
        AttributeType.getAttributeType(
          classOf[Integer],
          false
        ) === AttributeType.INTEGER
      )
    }

    it("should return LONG for Long class") {
      assert(
        AttributeType.getAttributeType(classOf[java.lang.Long], false) === AttributeType.LONG
      )
    }

    it("should return DOUBLE for Double class") {
      assert(
        AttributeType.getAttributeType(
          classOf[java.lang.Double],
          false
        ) === AttributeType.DOUBLE
      )
    }

    it("should return BOOLEAN for Boolean class") {
      assert(
        AttributeType.getAttributeType(
          classOf[java.lang.Boolean],
          false
        ) === AttributeType.BOOLEAN
      )
    }

    it("should return TIMESTAMP for Timestamp class") {
      assert(
        AttributeType.getAttributeType(
          classOf[Timestamp],
          false
        ) === AttributeType.TIMESTAMP
      )
    }

    it("should return BINARY for byte array class") {
      assert(
        AttributeType.getAttributeType(
          classOf[Array[Byte]],
          false
        ) === AttributeType.BINARY
      )
    }

    it("should return ANY for unknown types") {
      assert(
        AttributeType.getAttributeType(classOf[Object], false) === AttributeType.ANY
      )
      assert(
        AttributeType.getAttributeType(
          classOf[List[_]],
          false
        ) === AttributeType.ANY
      )
    }
  }
}
