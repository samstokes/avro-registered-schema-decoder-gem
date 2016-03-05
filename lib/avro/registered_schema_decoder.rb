require 'avro'
require 'schema_registry'

module Avro
  class RegisteredSchemaDecoder
    class Error < StandardError
      attr_reader :cause
      def initialize(message, cause = $!)
        super(message)
        @cause = cause
      end

      def to_s
        "#{super}#{" (caused by: #{cause})" if cause}"
      end
    end

    def initialize(registry_url, logger: nil)
      @registry = SchemaRegistry::Client.new(registry_url)
      @logger = logger
      @readers = {}
    end

    def decode_key(key_bytes)
      _, key = decode_key_with_schema(key_bytes)
      key
    end

    def decode_key_with_schema(key_bytes)
      # expected for unkeyed messages
      return if key_bytes.nil?

      decode_message(key_bytes)
    end

    def decode_value(value_bytes)
      _, value = decode_value_with_schema(value_bytes)
      value
    end

    def decode_value_with_schema(value_bytes)
      # expected, used to signify record deletion
      return if value_bytes.nil?
      # handle producers using older versions of librdkafka, which sent zero-
      # length messages instead of null messages:
      # see https://github.com/confluentinc/bottledwater-pg/pull/33
      return if value_bytes.empty?

      decode_message(value_bytes)
    end

    private
    # message format:
    #
    # Every message sent to Kafka is Avro-encoded, and prefixed with five
    # bytes:
    #  - The first byte is always 0, and reserved for future use.
    #  - The next four bytes are the schema ID in big-endian byte order.
    #
    # per https://github.com/confluentinc/bottledwater-pg/blob/8a11825/kafka/registry.c#L6-L8
    def decode_message(bytes)
      raise "Empty message" if bytes.empty?
      reserved, schema_id, avro = bytes.unpack('cNa*')
      raise "Reserved byte #{reserved.inspect} in message header (expected 0)!\nmessage: #{bytes.inspect}" unless 0 == reserved

      reader = reader_for(schema_id)
      decoder = Avro::IO::BinaryDecoder.new(StringIO.new(avro))

      wrap_error "parsing message with schema #{schema_id}" do
        [reader.writers_schema, reader.read(decoder)]
      end
    end

    def reader_for(schema_id)
      if @readers.key?(schema_id)
        return @readers[schema_id]
      end

      @logger.debug("Fetching schema #{schema_id}") if @logger
      schema = nil
      wrap_error "fetching schema #{schema_id}" do
        schema_json = @registry.schema(schema_id)
        schema = Avro::Schema.parse(schema_json)
      end

      Avro::IO::DatumReader.new(schema).tap do |reader|
        @readers[schema_id] = reader
      end
    end

    def wrap_error(doing_what)
      yield
    rescue
      raise Error, "error #{doing_what}"
    end
  end
end
