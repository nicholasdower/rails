# frozen_string_literal: true

require 'bundler/inline'

gemfile(true) do
  source 'https://rubygems.org'

  git_source(:github) { |repo| "https://github.com/#{repo}.git" }

  # Using main branch from 2023.05.06
  gem 'activerecord', github: 'rails/rails', ref: '74e24aef41da2482fa896684a7b4bccc6afc9b3b'
  gem 'minitest-reporters'
  gem 'mysql2'
end

require 'active_record'
require 'logger'
require 'minitest/autorun'
require 'minitest/reporters'

Minitest::Reporters.use! [Minitest::Reporters::SpecReporter.new()]

# Using the MySQL DB used by the repo unit tests.
ActiveRecord::Base.establish_connection(
  adapter:             'mysql2',
  host:                '127.0.0.1',
  port:                3306,
  username:            'root',
  database:            'activerecord_unittest',
  prepared_statements: false
)

ActiveRecord::Base.logger = Logger.new(STDOUT)

class Topic < ActiveRecord::Base; end

class OpenTransactionsTest < Minitest::Test
  def self.test_order; :alpha;end

  def setup
    ActiveRecord::Schema.define do
      create_table :topics, force: true do |t|
         t.string :title
         t.string :author_name
      end
    end
    Topic.create(id: 1, title: 'Original title', author_name: 'Original author')
  end

  # Test for the case where a rollback fails, then we fail to discard the transaction. The
  # transaction is unexpectedly left open and will be committed if another transaction is
  # attempted on the same connection.
  #
  # Output:
  #   Topic Load (0.4ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  #   TRANSACTION (0.1ms)   BEGIN
  #   Topic Update (0.3ms)  UPDATE `topics` SET `topics`.`title` = 'Updated title' WHERE `topics`.`id` = 1
  #   Topic Load (0.1ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  #   SQL (11.6ms)          SELECT title from topics where id = 1
  #   TRANSACTION (0.5ms)   BEGIN
  #   Topic Update (0.9ms)  UPDATE `topics` SET `topics`.`author_name` = 'Updated author' WHERE `topics`.`id` = 1
  #   TRANSACTION (0.4ms)   COMMIT
  #   Topic Load (0.7ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  def test_aborted_transaction_committed_when_rollback_raises_and_throw_away_raises
    connection = Topic.connection
    topic = Topic.find(1)

    # Update rollback_db_transaction to raise.
    Topic.connection.singleton_class.class_eval do
      alias :real_rollback_db_transaction :rollback_db_transaction
      define_method(:rollback_db_transaction) do
        raise 'rollback failed'
      end
    end

    # Update throw_away! to raise. Maybe unlikely, but possible.
    Topic.connection.singleton_class.class_eval do
      alias :real_throw_away! :throw_away!
      define_method(:throw_away!) do
        raise 'throw away failed'
      end
    end

    # Start a transaction, update a record, then roll back. The rollback and connection removal will fail.
    assert_raises(RuntimeError, 'rollback failed') do
      ActiveRecord::Base.transaction do
        topic.update(title: 'Updated title')
        raise ActiveRecord::Rollback
      end
    end
    assert connection.active?
    assert Topic.connection_pool.connections.include?(connection)

    # Any requests reusing the connection will see the uncommitted data.
    assert_equal 'Updated title', topic.reload.title

    # Any requests using a different connection will not see the uncommitted data.
    persisted_title = ActiveRecord::Base.connection_pool.checkout.exec_query(
      "SELECT title from topics where id = #{topic.id}"
    ).first['title']
    assert_equal 'Original title', persisted_title

    # Perform a new transaction. This will also commit the previously uncommitted changes.
    ActiveRecord::Base.transaction do
      topic.update(author_name: 'Updated author')
    end

    # Discard the connection to ensure anything we read was actually written to the database.
    Topic.connection.real_throw_away!

    # Both transactions were committed.
    assert_equal 'Updated title', topic.reload.title
    assert_equal 'Updated author', topic.author_name
  ensure
    ActiveRecord::Base.connection_handler.clear_all_connections!(:all)
  end

  # Test for the case where a commit fails, then the rollback fails. The transaction is
  # unexpectedly left open and will be committed if another transaction is attempted on
  # the same connection.
  #
  # Output:
  #   Topic Load (0.4ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  #   TRANSACTION (0.1ms)   BEGIN
  #   Topic Update (0.8ms)  UPDATE `topics` SET `topics`.`title` = 'Updated title' WHERE `topics`.`id` = 1
  #   Topic Load (0.2ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  #   SQL (14.2ms)          SELECT title from topics where id = 1
  #   TRANSACTION (0.5ms)   BEGIN
  #   Topic Update (0.2ms)  UPDATE `topics` SET `topics`.`author_name` = 'Updated author' WHERE `topics`.`id` = 1
  #   TRANSACTION (0.4ms)   COMMIT
  #   Topic Load (0.2ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  def test_failed_transaction_committed_when_commit_raises_and_rollback_raises
    connection = Topic.connection
    topic = Topic.find(1)

    # Update commit_db_transaction to raise the first time it is called.
    Topic.connection.singleton_class.class_eval do
      alias :real_commit_db_transaction :commit_db_transaction
      define_method(:commit_db_transaction) do
        unless @ran_once
          @ran_once = true
          raise 'commit failed'
        end
        real_commit_db_transaction
      end
    end

    # Update rollback_transaction to raise.
    Topic.connection.transaction_manager.singleton_class.class_eval do
      alias :real_rollback_transaction :rollback_transaction
      define_method(:rollback_transaction) do |*_args|
        raise 'rollback failed'
      end
    end

    # Start a transaction and update a record. The commit and rollback will fail.
    assert_raises(RuntimeError, 'rollback failed') do
      ActiveRecord::Base.transaction do
        topic.update(title: 'Updated title')
      end
    end
    assert connection.active?
    assert Topic.connection_pool.connections.include?(connection)

    # Any request reusing the connection will see the uncommitted data.
    assert_equal 'Updated title', topic.reload.title

    # Any requests using a different connection will not see the uncommitted data.
    persisted_title = ActiveRecord::Base.connection_pool.checkout.exec_query(
      "SELECT title from topics where id = #{topic.id}"
    ).first['title']
    assert_equal 'Original title', persisted_title

    # Perform a new transaction. This will also commit the previously uncommitted changes.
    ActiveRecord::Base.transaction do
      topic.update(author_name: 'Updated author')
    end

    # Discard the connection to ensure anything we read was actually written to the databse.
    Topic.connection.throw_away!

    # Both transactions were committed.
    assert_equal 'Updated title', topic.reload.title
    assert_equal 'Updated author', topic.author_name
  ensure
    ActiveRecord::Base.connection_handler.clear_all_connections!(:all)
  end

  # Test for the case where a rollback fails, then we fail to discard the transaction. The
  # transaction is unexpectedly left open, allowing subsequent transactions to be silently
  # discarded.
  #
  # Output:
  #   Topic Load (0.2ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  #   TRANSACTION (0.1ms)   BEGIN
  #   Topic Update (0.2ms)  UPDATE `topics` SET `topics`.`title` = 'Updated title' WHERE `topics`.`id` = 1
  #   Topic Load (0.2ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  #   SQL (11.1ms)          SELECT title from topics where id = 1
  #   Topic Update (0.2ms)  UPDATE `topics` SET `topics`.`author_name` = 'Updated author' WHERE `topics`.`id` = 1
  #   Topic Load (0.3ms)    SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  def test_subsequent_transaction_silently_discarded_after_rollback_raises_and_throw_away_raises
    connection = Topic.connection
    topic = Topic.find(1)

    # Update rollback_transaction to raise.
    Topic.connection.transaction_manager.singleton_class.class_eval do
      alias :real_rollback_transaction :rollback_transaction
      define_method(:rollback_transaction) do
        raise 'rollback failed'
      end
    end

    # Update throw_away! to raise. Maybe unlikely, but possible.
    Topic.connection.singleton_class.class_eval do
      alias :real_throw_away! :throw_away!
      define_method(:throw_away!) do
        raise 'throw away failed'
      end
    end

    # Start a transaction, update a record, then roll back. The rollback and connection removal will fail.
    assert_raises(RuntimeError, 'rollback failed') do
      ActiveRecord::Base.transaction do
        topic.update(title: 'Updated title')
        raise ActiveRecord::Rollback
      end
    end
    assert connection.active?
    assert Topic.connection_pool.connections.include?(connection)

    # Any request reusing the connection will see the uncommitted data.
    assert_equal 'Updated title', topic.reload.title

    # Any requests using a different connection will not see the uncommitted data.
    persisted_title = ActiveRecord::Base.connection_pool.checkout.exec_query(
      "SELECT title from topics where id = #{topic.id}"
    ).first['title']
    assert_equal 'Original title', persisted_title

    # Perform a new transaction. This will not be committed because we are still in the previous transaction.
    ActiveRecord::Base.transaction do
      topic.update(author_name: 'Updated author')
    end

    # Discard the connection to ensure anything we read was actually written to the databse.
    Topic.connection.real_throw_away!

    # Nothing was committed.
    assert_equal 'Original title', topic.reload.title
    assert_equal 'Original author', topic.author_name
  ensure
    ActiveRecord::Base.connection_handler.clear_all_connections!(:all)
  end

  # Test for the case where beginning a transaction succeeds, but then the begin method
  # raises. The transaction is unexpectedly left open, allowing subsequent writes to
  # silently fail.
  #
  # Output:
  #   Topic Load (0.2ms)   SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  #   TRANSACTION (0.1ms)  BEGIN
  #    (0.2ms)             UPDATE topics SET title = 'Updated title' WHERE id = 1;
  #   Topic Load (0.2ms)   SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  #   SQL (12.6ms)         SELECT title from topics where id = 1
  #   Topic Load (0.3ms)   SELECT `topics`.* FROM `topics` WHERE `topics`.`id` = 1 LIMIT 1
  def test_write_silently_discarded_after_beginning_a_transaction_then_raising
    connection = Topic.connection
    topic = Topic.find(1)

    # Disable lazy transactions so that we will begin a transaction immediately, before writing.
    connection.disable_lazy_transactions!

    # Update begin_transaction to raise after successfully beginning a transaction.
    Topic.connection.transaction_manager.singleton_class.class_eval do
      alias :real_begin_transaction :begin_transaction
      define_method(:begin_transaction) do |*_args|
        real_begin_transaction
        raise 'begin failed'
      end
    end

    # Begin a transaction. This will raise but no rollback will be performed and the connection will not be removed.
    assert_raises(RuntimeError, 'begin failed') do
      ActiveRecord::Base.transaction { }
    end
    assert connection.active?
    assert Topic.connection_pool.connections.include?(connection)

    # Use the connection to execute a statement. Since we are still in a transaction, this will not be committed.
    Topic.connection.execute("UPDATE topics SET title = 'Updated title' WHERE id = #{topic.id};")

    # Any request reusing the connection will see the uncommitted data.
    assert_equal 'Updated title', topic.reload.title

    # Any requests using a different connection will not see the uncommitted data.
    persisted_title = ActiveRecord::Base.connection_pool.checkout.exec_query(
      "SELECT title from topics where id = #{topic.id}"
    ).first['title']
    assert_equal 'Original title', persisted_title

    # Discard the connection.
    Topic.connection.throw_away!

    # Nothing was committed.
    assert_equal 'Original title', topic.reload.title
  ensure
    ActiveRecord::Base.connection_handler.clear_all_connections!(:all)
  end
end
