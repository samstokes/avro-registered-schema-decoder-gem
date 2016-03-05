Gem::Specification.new do |gem|
  gem.name = 'avro-registered-schema-decoder'
  gem.version = '0.1.0'

  gem.summary = 'Decodes messages encoded as prefixed Avro, looking up schemas in a schema registry'
  gem.description = <<-DESC
Provides a decoder for messages encoded as "prefixed Avro", where the binary
Avro encoding is prefixed with a schema id referring to a schema stored in a
Confluent Schema Registry
(http://docs.confluent.io/1.0/schema-registry/docs/intro.html).
  DESC

  gem.authors = ['Sam Stokes']
  gem.email = %w(me@samstokes.co.uk)
  gem.homepage = 'https://github.com/samstokes/avro-registered-schema-decoder'
  gem.license = 'MIT'


  gem.add_dependency 'avro'
  gem.add_dependency 'schema_registry'

  gem.add_development_dependency 'bundler'
  gem.add_development_dependency 'rake'
  gem.add_development_dependency 'rspec'


  gem.files = Dir[*%w(
      lib/**/*
      README*)] & %x{git ls-files -z}.split("\0")
end
