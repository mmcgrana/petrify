class PetrifySimple
  attr_reader :path, :snapshot_interval, :max_upload_time, :put_url, :quiet

  def initialize(opts)
    @path = opts[:path]
    @snapshot_interval = opts[:snapshot_interval]
    @max_upload_time = snapshot_interval / 2
    @put_url = opts[:put_url]
    @quiet = opts[:quiet]
  end

  def put
    start = Time.now
    stat = File.stat(path)
    log("put event=start ino=#{stat.ino} size=#{stat.size}")
    out = `curl --silent --max-time #{max_upload_time} --write-out %{http_code} --data-binary @#{path} --header 'Content-Type:' --request PUT --url '#{put_url}' 2>&1`.chomp
    exit_status = $?.exitstatus
    elapsed = Time.now - start
    success = ((out == "200") && (exit_status == 0))
    log("put event=finish success=#{success} out=#{out} exit_status=#{exit_status} elapsed=#{elapsed}")
  end

  def persist
    log("persist event=start path=#{path} snapshot_interval=#{snapshot_interval}")
    trap("TERM") do
      log("persist event=trap")
      put
      log("persist event=exit")
      exit(0)
    end
    loop do
      log("persist event=tick")
      put
      log("persist event=sleep")
      sleep(snapshot_interval)
    end
  end

  def log(msg)
    puts("petrify_simple #{msg}") if !quiet
  end
end
