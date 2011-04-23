require "rubygems"
require "json"
require "aws/s3"

class Petrify
  attr_reader :path, :bucket, :prefix, :delta_interval, :snapshot_interval, :max_upload_time, :at_timestamp, :aws_access_key_id, :aws_secret_access_key, :quiet
  attr_reader :prev_ino, :prev_history, :prev_size

  def initialize(opts)
    @path = opts[:path]
    @bucket = opts[:bucket]
    @prefix = opts[:prefix]
    @delta_interval = opts[:delta_interval]
    @snapshot_interval = opts[:snapshot_interval]
    @max_upload_time = (delta_interval / 2) if delta_interval
    @at_timestamp = opts[:at_timestamp]
    @aws_access_key_id = opts[:aws_access_key_id]
    @aws_secret_access_key = opts[:aws_secret_access_key]
    @quiet = opts[:quiet]
    connect
  end

  def connect
    AWS::S3::Base.establish_connection!(:access_key_id => aws_access_key_id, :secret_access_key => aws_secret_access_key)
  end

  def put_data(file, timestamp, ino, offset, limit)
    start = Time.now
    log("put_data event=start timestamp=#{timestamp} ino=#{ino} offset=#{offset} limit=#{limit}")
    success =
      begin
        file.seek(offset)
        data = file.read(limit)
        AWS::S3::S3Object.store("#{prefix}/data/#{timestamp}", data, bucket)
        true
      rescue => e
        log("put_data event=error class=#{e.class} message='#{e.message}'")
        false
      end
    elapsed = Time.now - start
    log("put_data event=finish success=#{success} elapsed=#{elapsed}")
    success
  end

  def put_meta(timestamp, meta)
    start = Time.now
    log("put_meta event=start timestamp=#{timestamp}")
    success =
      begin
        meta_json = JSON.dump(meta)
        AWS::S3::S3Object.store("#{prefix}/meta/#{timestamp}", meta_json, bucket)
        true
      rescue => e
        log("put_data event=error class=#{e.class} message='#{e.message}'")
        false
      end
    elapsed = Time.now - start
    log("put_meta event=finish success=#{success} elapsed=#{elapsed}")
    success
  end

  def tick
    log("tick event=open")
    file = File.open(path, "r")
    stat = file.stat
    ino = stat.ino
    size = stat.size
    timestamp = Time.now.to_i
    log("tick event=stat prev_ino=#{prev_ino} ino=#{ino} prev_size=#{prev_size} size=#{size}")
    if (ino != prev_ino)
      log("tick event=snapshot")
      if put_data(file, timestamp, ino, 0, size)
        history = [timestamp]
        meta = {"history" => history}
        if put_meta(timestamp, meta)
          @prev_history = history
          @prev_ino = ino
          @prev_size = size
        end
      end
    elsif (size != prev_size)
      log("tick event=delta")
      if put_data(file, timestamp, ino, prev_size, size - prev_size)
        history = prev_history + [timestamp]
        meta = {"history" => history}
        if put_meta(timestamp, meta)
          @prev_history = history
          @prev_ino = ino
          @prev_size = size
        end
      end
    else
      log("tick event=unchanged")
    end
    file.close
  end

  def persist
    log("persist event=start path=#{path} prefix=#{prefix} delta_interval=#{delta_interval}")
    trap("TERM") do
      log("persist event=trap")
      tick
      log("persist event=exit")
      exit(0)
    end
    loop do
      log("persist event=tick")
      tick
      log("persist event=sleep")
      sleep(delta_interval)
    end
  end

  def get_list
    meta_prefix = "#{prefix}/meta/"
    AWS::S3::Bucket.objects(bucket, :prefix => meta_prefix).map do |o|
      timestamp = o.key.sub(meta_prefix, "").to_i
    end
  end

  def list
    get_list.each do |timestamp|
      time = Time.at(timestamp)
      puts("#{timestamp}    #{time}")
    end
  end

  def get_meta(timestamp)
    start = Time.now
    log("get_meta event=start timestamp=#{timestamp}")
    meta_json = AWS::S3::S3Object.value("#{prefix}/meta/#{timestamp}", bucket)
    meta = JSON.parse(meta_json)
    log("get_meta event=finish elapsed=#{Time.now - start}")
    meta
  end

  def get_data(file, timestamp)
    start = Time.now
    log("get_data event=start timestamp=#{timestamp}")
    data = AWS::S3::S3Object.value("#{prefix}/data/#{timestamp}", bucket)
    file.write(data)
    log("get_data event=finish elapsed=#{Time.now - start}")
  end

  def recover
    start = Time.now
    log("recover event=start path=#{path} prefix=#{prefix} at_timestamp=#{at_timestamp || "last"}")
    if File.exists?(path)
      log("recover event=abort reason=exists")
      exit(1)
    end
    log("recover event=open")
    file = File.open(path, "w")
    if !at_timestamp
      log("recover event=find")
      at_timestamp = get_list.sort.last
    end
    meta = get_meta(at_timestamp)
    history = meta["history"]
    log("recover event=build history_size=#{history.size}")
    history.each { |timestamp| get_data(file, timestamp) }
    log("recover event=close")
    file.close
    log("recover event=finish elapsed=#{Time.now - start}")
  end

  def log(msg)
    puts("petrify #{msg}") if !quiet
  end
end
