require 'httparty'
require 'time'
require 'byebug'

COINGECKO_API_KEY = 'XXXXXXXX' # Replace with your actual key
COINGECKO_API_URL = 'https://api.coingecko.com/api/v3'

def fetch_long_term_ton_cad_prices(start_date, end_date)
  from_ts = start_date.to_time.to_i
  to_ts = end_date.to_time.to_i

  response = HTTParty.get(
    "#{COINGECKO_API_URL}/coins/the-open-network/market_chart/range",
    query: {
      vs_currency: 'cad',
      from: from_ts,
      to: to_ts
    },
    headers: { 'x_cg_demo_api_key' => COINGECKO_API_KEY }
  )

  if response.success?
    byebug
    prices = response['prices'] || []
    daily_prices = prices.map do |ts_price|
      time = Time.at(ts_price[0] / 1000).to_date
      price = ts_price[1].to_f
      [time, price]
    end.to_h

    daily_prices
  else
    puts "Error: #{response.code} - #{response.body}"
    {}
  end
end

# Example:
# Fetch prices from Jan 2022 to today
puts fetch_long_term_ton_cad_prices(Date.new(2022,1,1), Date.today)
