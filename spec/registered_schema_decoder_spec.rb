require 'avro/registered_schema_decoder'

describe Avro::RegisteredSchemaDecoder do
  before(:example) do
    @mock_schema_registry = double('schema_registry')
    allow(SchemaRegistry::Client).to receive(:new).
      and_return(@mock_schema_registry)
  end

  subject { described_class.new('<fake url>') }

  INT_SCHEMA = %(
    {
      "type":"record",
      "name":"numbers",
      "fields":[
        {
          "name":"number",
          "type":["null","int"]
        }
      ]
    }
  )

  STRING_SCHEMA = %(
    {
      "type":"record",
      "name":"logs",
      "fields":[
        {
          "name":"message",
          "type":["null","string"]
        }
      ]
    }
  )

  describe '#decode_key' do
    example 'decodes records with int fields' do
      expect(@mock_schema_registry).to receive(:schema).with(1).
        and_return(INT_SCHEMA)

      record = subject.decode_key("\x00\x00\x00\x00\x01\x02\x0E")
      expect(record.fetch('number')).to eq(7)
    end

    example 'decodes records with string fields' do
      expect(@mock_schema_registry).to receive(:schema).with(7).
        and_return(STRING_SCHEMA)

      record = subject.decode_key("\x00\x00\x00\x00\a\x02$Launching missiles")
      expect(record.fetch('message')).to eq('Launching missiles')
    end

    example 'returns nil for nil keys' do
      result = subject.decode_key(nil)
      expect(result).to be_nil
    end

    example 'raises an informative error if the schema could not be retrieved' do
      expect(@mock_schema_registry).to receive(:schema).with(1).
        and_raise(SchemaRegistry::ResponseError.new(404, 'oops'))

      expect { subject.decode_key("\x00\x00\x00\x00\x01\x02\x0E") }.
        to raise_error(Avro::RegisteredSchemaDecoder::Error, /oops/)
    end
  end

  describe '#decode_value' do
    example 'decodes records with int fields' do
      expect(@mock_schema_registry).to receive(:schema).with(1).
        and_return(INT_SCHEMA)

      record = subject.decode_value("\x00\x00\x00\x00\x01\x02\x0E")
      expect(record.fetch('number')).to eq(7)
    end

    example 'decodes records with string fields' do
      expect(@mock_schema_registry).to receive(:schema).with(7).
        and_return(STRING_SCHEMA)

      record = subject.decode_value("\x00\x00\x00\x00\a\x02$Launching missiles")
      expect(record.fetch('message')).to eq('Launching missiles')
    end

    example 'returns nil for nil values' do
      result = subject.decode_value(nil)
      expect(result).to be_nil
    end

    # handle producers using older versions of librdkafka, which sent zero-
    # length messages instead of null messages:
    # see https://github.com/confluentinc/bottledwater-pg/pull/33
    example 'returns nil for zero-length values' do
      result = subject.decode_value('')
      expect(result).to be_nil
    end

    example 'raises an informative error if the schema could not be retrieved' do
      expect(@mock_schema_registry).to receive(:schema).with(1).
        and_raise(SchemaRegistry::ResponseError.new(404, 'oops'))

      expect { subject.decode_value("\x00\x00\x00\x00\x01\x02\x0E") }.
        to raise_error(Avro::RegisteredSchemaDecoder::Error, /oops/)
    end
  end

end
