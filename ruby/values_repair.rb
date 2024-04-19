# frozen_string_literal: true

# value repair script
require 'io/console'
require 'redis'
require 'redis-clustering'
require 'sqlite3'
require 'benchmark'

SLICE_SIZE = 10
PATH_TO_SQL_DB = 'result.db.3'

p 'source redis url (includes rediss:// and port):'
source_redis_endpoint = $stdin.noecho(&:gets).chomp

p 'target memorydb url:'
target_memorydb_endpoint = $stdin.noecho(&:gets).chomp

p 'dryrun? (y/n)'
dryrun = gets.chomp == 'y'

source_elasticache = Redis.new(url: source_redis_endpoint)
target_memdb = Redis::Cluster.new(nodes: [target_memorydb_endpoint], reconnect_attempts: 3, timeout: 0.1,
                                  slow_command_timeout: 0.1)

count_outcomes = []

p "#{Time.now} START"

Benchmark.bm do |benchmark|
  db = SQLite3::Database.open PATH_TO_SQL_DB
  db.results_as_hash = true

  # 1. repair string values
  benchmark.report('Repair Strings') do
    results = db.query "select key from key where type = 'string';"
    if results.respond_to?(:size)
      p "Found #{results.size} string keys to repair"

      results.each_slice(SLICE_SIZE) do |slice|
        p "Starting repair of new slice of #{slice.size} string keys"

        keys = slice.each { |r| r['key'] }
        elasticache_vals = source_elasticache.mget(*keys)
        # zip and flatten so in the order of 'k1', 'v1', 'k2', 'v2'
        mset_args = keys.zip(elasticache_vals).flatten
        # set memdb value as
        target_memdb.mset(mset_args) unless dryrun

        p "Repaired #{keys.size} keys"
        p ''
      end

      p 'Finished all string keys repair'

      count_outcomes.push(results.size)
    else
      results&.close
      p 'No rows returned for strings'
    end
  end

  # 2. repair set values
  benchmark.report('Repair Sets') do
    set_results = db.query "select key from key where type = 'set';"
    if set_results.respond_to?(:size)
      p "Found #{set_results.size} set keys to repair"

      set_results.each do |row|
        p 'Starting repair of new set...'

        k = row['key']
        set_members = source_elasticache.smembers(k)
        # remove and re-add the set members to the k
        unless dryrun
          target_memdb.multi do |multi|
            multi.del(k)
            multi.sadd(k, set_members)
          end
        end

        p 'Finished repair of 1 set'
        p ''
      end

      p 'Finished set keys repair'

      count_outcomes.push(set_results.size)
    else
      set_results&.close
      p 'No rows returned for sets'
    end
  end

  # 3. repair hash values
  benchmark.report('Repair Hashes') do
    hash_results = db.query "select key from key where type = 'hash';"
    if hash_results.respond_to?(:size)
      p "Found #{hash_results.size} hash keys to repair"

      hash_results.each do |row|
        p 'Starting repair of new hash...'

        k = row['key']
        hval = elasticache.hgetall(k)
        # set memdb value as the hash value of k
        unless dryrun
          target_memdb.multi do |multi|
            multi.del(k)
            multi.hset(k, hval)
          end
        end

        p 'Finished repair of hash'
        p ''
      end

      count_outcomes.push(hash_results.size)
    else
      hash_results&.close
      p 'No rows returned for hashes'
    end
  end
rescue SQLite3::Exception => e
  p "Error opening database: #{e}"
ensure
  db&.close
end

p ''
p "#{Time.now} FINISHED"
p "number of Strings repaired if not dryrun: #{count_outcomes[0]}"
p "number of Sets repaired if not dryrun: #{count_outcomes[1]}"
p "number of Hashes repaired if not dryrun: #{count_outcomes[2]}"
