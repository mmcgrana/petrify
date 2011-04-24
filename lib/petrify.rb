require "rubygems"
require "bundler"
Bundler.setup
require "json"
require "aws/s3"

class Petrify
  attr_reader :path, :bucket, :prefix, :interval, :at_timestamp, :aws_access_key_id, :aws_secret_access_key, :quiet
  attr_reader :persisted_meta

  def initialize(opts)
    @path = opts[:path]
    @bucket = opts[:bucket]
    @prefix = opts[:prefix]
    @interval = opts[:interval]
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
    timestamp = Time.now.to_i
    file = File.open(path, "r")
    stat = file.stat
    ino = stat.ino
    size = stat.size
    persisted_ino = (persisted_meta[-1] && (persisted_meta[-1]["ino"]))
    persisted_size = (persisted_meta[-1] && (persisted_meta[-1]["offset"] + persisted_meta[-1]["limit"]))
    log("tick event=stat timestamp=#{timestamp} ino=#{ino} persisted_ino=#{persisted_ino} size=#{size} persisted_size=#{size}")

    if (ino != persisted_ino)
      log("tick event=snapshot")
      if put_data(file, timestamp, ino, 0, size)
        meta_elem = {"timestamp" => timestamp, "ino" => ino, "offset" => 0, "limit" => size}
        new_meta = [meta_elem]
        if put_meta(timestamp, new_meta)
          @persisted_meta = new_meta
        end
      end
    elsif (size != persisted_size)
      log("tick event=delta")
      offset = persisted_size
      limit = size - persisted_size
      if put_data(file, timestamp, ino, offset, limit)
        meta_elem = {"timestamp" => timestamp, "ino" => ino, "offset" => offset, "limit" => limit}
        new_meta = persisted_meta + [meta_elem]
        if put_meta(timestamp, new_meta)
          @persisted_meta = new_meta
        end
      end
    else
      log("tick event=unchanged")
    end

    file.close
  end

  def persist
    log("persist event=start bucket=#{bucket} path=#{path} prefix=#{prefix} interval=#{interval}")
    @persisted_meta = []
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
      sleep(interval)
    end
  end

  def get_list
    start = Time.now
    log("get_list event=start")
    meta_prefix = "#{prefix}/meta/"
    list = AWS::S3::Bucket.objects(bucket, :prefix => meta_prefix).map do |o|
      timestamp = o.key.sub(meta_prefix, "").to_i
    end
    log("get_list event=finish elapsed=#{Time.now - start}")
    list
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

  def list
    start = Time.now
    log("list event=start")
    get_list.each do |timestamp|
      time = Time.at(timestamp)
      log("list event=emit timestamp=#{timestamp} time='#{time}'")
    end
    log("list event=finish elapsed=#{Time.now - start}")
  end

  def show
    start = Time.now
    log("show event=start")
    tail_timestamp =
      if !at_timestamp
        log("show event=find")
        get_list.sort.last
      else
        at_timestamp
      end
    meta = get_meta(tail_timestamp)
    meta.each do |e|
      log("show event=emit timestamp=#{e["timestamp"]} ino=#{e["ino"]} offset=#{e["offset"]} limit=#{e["limit"]}")
    end
    log("show event=finish elapsed=#{Time.now - start}")
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
    tail_timestamp =
      if !at_timestamp
        log("recover event=find")
        get_list.sort.last
      else
        at_timestamp
      end
    meta = get_meta(tail_timestamp)
    log("recover event=build chunks=#{meta.size}")
    meta.each { |e| get_data(file, e["timestamp"]) }
    log("recover event=close")
    file.close
    log("recover event=finish elapsed=#{Time.now - start}")
  end

  def log(msg)
    puts("petrify #{msg}") if !quiet
  end
end
