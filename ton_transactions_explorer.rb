require 'httparty'
require 'csv'
require 'json'
require 'time'
require 'byebug'


# Config
wallet_address = 'EQBl3gg6AAdjgjO2ZoNU5Q5EzUIl8XMNZrix8Z5dJmkHUfxI'
anton_url = "https://anton.tools/api/v0/transactions?address=#{wallet_address}&workchain=0&order=DESC&limit=100"

begin
  response = HTTParty.get(anton_url)
  raise "API request failed: #{response.code} - #{response.body}" unless response.success?

  data = JSON.parse(response.body)
  transactions = data['results'] || []

  puts "Fetched #{transactions.size} transactions"

  CSV.open('ton_tax_report.csv', 'w') do |csv|
    csv << ['Date (UTC)', 'Tx Hash', 'Type', 'Amount (TON)', 'Fee (TON)', 'Counterparty Address']

    transactions.each do |tx|
      date = tx['created_at'] ? Time.parse(tx['created_at']).utc.strftime("%Y-%m-%d %H:%M:%S") : 'N/A'
      hash = tx['hash'] || 'N/A'
      fee = (tx['total_fees'].to_f / 1_000_000_000).round(9)

      if tx['in_amount'].to_f > 0
        type = 'IN'
        amount = (tx['in_amount'].to_f / 1_000_000_000).round(9)
        counterparty = tx['in_msg_hash'] || 'Unknown'
      elsif tx['out_amount'].to_f > 0
        type = 'OUT'
        amount = (tx['out_amount'].to_f / 1_000_000_000).round(9)
        counterparty = tx['out_msg'] || 'Unknown'
      else
        type = 'UNKNOWN'
        amount = 0
        counterparty = 'N/A'
      end

      csv << [date, hash, type, amount, fee, counterparty]
    end
  end

  puts "Saved to ton_tax_report.csv"

rescue => e
  puts "Error: #{e.message}"
end
