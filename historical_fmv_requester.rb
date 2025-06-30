require 'csv'
require 'yaml'
require 'httparty'
require 'digest'
require 'parallel'
require 'date'

# Configuration
COINGECKO_API_KEY = 'XXXX' # your coingecko api key here
COINGECKO_API_URL = 'https://api.coingecko.com/api/v3'
CACHE_DIR = 'cache'
LOG_DIR = 'logs'
CSV_DIR = 'CSV_Files'
MIN_API_INTERVAL = 12.0 # seconds (ensures max 10 calls/minute)
MAX_RETRIES = 3
MAX_HISTORY_DAYS = 365
MAX_THREADS = 2
BASE_RETRY_DELAY = 10 # Additional seconds to wait after failed attempts

CHAIN_MAPPING = [
  { name: 'opbnb', patterns: [/opbnb/i], id: 'opbnb', currency: 'cad' },
  { name: 'scroll', patterns: [/scroll/i, /scr/i], id: 'scroll', currency: 'cad' },
  { name: 'bsc', patterns: [/bsc/i, /binance/i], id: 'binancecoin', currency: 'cad' },
  { name: 'sol', patterns: [/sol/i, /solana/i], id: 'solana', currency: 'cad' },
  { name: 'base', patterns: [/base/i], id: 'base', currency: 'cad' },
  { name: 'arb', patterns: [/arb/i, /arbitrum/i], id: 'arbitrum', currency: 'cad' },
  { name: 'eth', patterns: [/eth/i, /ethereum/i], id: 'ethereum', currency: 'cad' },
  { name: 'ton', patterns: [/ton/i], id: 'the-open-network', currency: 'cad' },
  { name: 'op', patterns: [/op/i, /opt/i, /opti/i, /optimism/], id: 'optimism', currency: 'cad' },
  { name: 'lin', patterns: [/lin/i, /linea/], id: 'linea', currency: 'cad' },
  { name: 'sonic', patterns: [/sonic/], id: 'sonic', currency: 'cad' },
  { name: 'zksync', patterns: [/zksync/], id: 'zksync', currency: 'cad' }
]

class RateLimiter
  def initialize(interval)
    @interval = interval
    @mutex = Mutex.new
    @last_call = Time.now - interval
  end

  def wait
    @mutex.synchronize do
      elapsed = Time.now - @last_call
      if elapsed < @interval
        sleep_time = @interval - elapsed
        log("Rate limiting - sleeping #{sleep_time.round(2)}s")
        sleep(sleep_time)
      end
      @last_call = Time.now
    end
  end
end

$rate_limiter = RateLimiter.new(MIN_API_INTERVAL)
$date_cache = {} # Global cache to track dates being processed

def ensure_directories
  [LOG_DIR, CACHE_DIR].each do |dir|
    unless Dir.exist?(dir)
      puts "[#{Time.now.strftime("%H:%M:%S")}] Creating directory: #{dir}"
      Dir.mkdir(dir)
    end
  end
end

def log(message, level = :info)
  timestamp = Time.now.strftime("%H:%M:%S")
  log_entry = "[#{timestamp}] #{message}"

  # Console output
  case level
  when :error
    puts "\e[31m#{log_entry}\e[0m"
  when :warn
    puts "\e[33m#{log_entry}\e[0m"
  else
    puts log_entry
  end

  # File output
  begin
    File.write("#{LOG_DIR}/processing_#{Date.today}.log", "#{log_entry}\n", mode: 'a')
  rescue => e
    puts "\e[31m[#{timestamp}] Failed to write to log file: #{e.message}\e[0m"
  end
end

def safe_parse_date(value, row)
  return nil if value.nil? || value.empty?

  if value.match?(/^\d+$/)
    begin
      return Time.at(value.to_i).to_date
    rescue ArgumentError; end
  end

  begin
    return DateTime.iso8601(value).to_date
  rescue ArgumentError; end

  begin
    return DateTime.parse(value).to_date
  rescue ArgumentError => e
    log("Failed to parse date: #{value}", :warn)
    nil
  end
end

def detect_blockchain(filename)
  filename = filename.downcase
  CHAIN_MAPPING.each do |config|
    config[:patterns].each do |pattern|
      if filename.match?(pattern)
        log("Chain detected: #{config[:name]}")
        return {
          name: config[:name],
          id: config[:id],
          currency: config[:currency],
          file_prefix: "historical_fmv_#{config[:name]}_#{config[:currency]}"
        }
      end
    end
  end
  log("Cannot detect blockchain", :warn)
  nil
end

def fetch_historical_price(coin_id, date, currency)
  cache_key = "#{coin_id}_#{date}"

  # Check if we're already processing this date
  if $date_cache[cache_key] == :processing
    log("Already processing #{coin_id} for #{date}, waiting...")
    while $date_cache[cache_key] == :processing
      sleep 1
    end
    return $date_cache[cache_key]
  end

  retries = 0
  price = nil

  # Mark this date as being processed
  $date_cache[cache_key] = :processing

  while retries <= MAX_RETRIES
    $rate_limiter.wait

    begin
      log("Attempt #{retries + 1}/#{MAX_RETRIES + 1}: Fetching #{coin_id} for #{date}")

      response = HTTParty.get(
        "#{COINGECKO_API_URL}/coins/#{coin_id}/history",
        query: { date: date.strftime('%d-%m-%Y'), localization: false },
        headers: { 'x_cg_demo_api_key' => COINGECKO_API_KEY },
        timeout: 10
      )

      if response.success?
        price = response.dig('market_data', 'current_price', currency)
        if price
          log("Successfully fetched #{currency.upcase} rate for #{coin_id} on #{date}: #{price}")
          $date_cache[cache_key] = price.to_f
          return price.to_f
        else
          log("No price data found for #{coin_id} on #{date}", :warn)
          $date_cache[cache_key] = nil
          return nil
        end
      else
        log("API request failed with code #{response.code}", :warn)
      end
    rescue => e
      log("API error: #{e.message}", :warn)
    end

    retries += 1
    if retries <= MAX_RETRIES
      additional_wait = BASE_RETRY_DELAY * retries
      log("Waiting #{additional_wait}s before retry #{retries + 1}/#{MAX_RETRIES + 1} for #{coin_id} on #{date}")
      sleep(additional_wait)
    end
  end

  log("Max retries (#{MAX_RETRIES}) exceeded for #{coin_id} on #{date}", :error)
  $date_cache[cache_key] = nil
  nil
end

begin
  ensure_directories
  log("Initializing FMV Processor")
  log("API Key: #{COINGECKO_API_KEY[0..4]}...#{COINGECKO_API_KEY[-4..-1]}")

  all_files = Dir.glob("#{CSV_DIR}/*.csv")
  log("Found #{all_files.size} CSV files in #{CSV_DIR}")

  processed_files = all_files.reject do |file|
    processed_marker = "#{CACHE_DIR}/processed_#{Digest::SHA256.file(file).hexdigest}"
    if File.exist?(processed_marker)
      log("Skipping already processed file: #{File.basename(file)}")
      true
    else
      false
    end
  end

  log("Processing #{processed_files.size} new files")
  chain_results = Hash.new { |h,k| h[k] = {rates: 0, missing: 0} }

  Parallel.each(processed_files, in_threads: MAX_THREADS) do |file|
    file_basename = File.basename(file)
    begin
      log("Processing file: #{file_basename}")

      chain = detect_blockchain(file_basename)
      next unless chain

      cache_file = "#{CACHE_DIR}/#{chain[:file_prefix]}.yaml"
      missing_file = "#{CACHE_DIR}/#{chain[:file_prefix]}_missing.yaml"

      cache = File.exist?(cache_file) ? YAML.safe_load(File.read(cache_file), permitted_classes: [Date]) || {} : {}
      missing = File.exist?(missing_file) ? YAML.safe_load(File.read(missing_file), permitted_classes: [Date]) || {} : {}

      transactions = []
      CSV.foreach(file, headers: true) do |row|
        date_str = row['Date (UTC)'] || row['DateTime (UTC)'] || row['Block Time'] || row['Human Time']
        date = safe_parse_date(date_str, row)
        transactions << {row: row, date: date} if date
      end

      log("Found #{transactions.size} valid transactions in #{file_basename}")

      # Group transactions by date to avoid duplicate API calls
      dates = transactions.map { |tx| tx[:date] }.uniq

      new_rates = {}
      new_missing = {}

      dates.each do |date|
        next if date < (Date.today - MAX_HISTORY_DAYS)

        if cache.key?(date)
          log("Using cached rate for #{chain[:name]} on #{date}")
          next
        end

        if missing.key?(date)
          log("Skipping previously missing date #{date} for #{chain[:name]}")
          next
        end

        price = fetch_historical_price(chain[:id], date, chain[:currency])
        if price
          new_rates[date] = price
          chain_results[chain[:name]][:rates] += 1
        else
          new_missing[date] = 'missing'
          chain_results[chain[:name]][:missing] += 1
        end
      end

      unless new_rates.empty?
        File.write(cache_file, cache.merge(new_rates).to_yaml)
        log("Added #{new_rates.size} new rates for #{chain[:name]} to #{cache_file}")
      end

      unless new_missing.empty?
        File.write(missing_file, missing.merge(new_missing).to_yaml)
        log("Added #{new_missing.size} missing dates for #{chain[:name]} to #{missing_file}")
      end

      File.write("#{CACHE_DIR}/processed_#{Digest::SHA256.file(file).hexdigest}", '')
      log("Completed processing #{file_basename}")

    rescue => e
      log("Error processing file #{file_basename}: #{e.message}", :error)
    end
  end

  log("\nProcessing complete. Results by chain:")
  chain_results.each do |chain, counts|
    log("#{chain.upcase}: #{counts[:rates]} rates found | #{counts[:missing]} missing dates")
  end

rescue => e
  log("Fatal error: #{e.message}", :error)
  exit(1)
end
