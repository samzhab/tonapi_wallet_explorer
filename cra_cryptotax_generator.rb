require 'csv'
require 'time'

# Create output directories
Dir.mkdir('CRA_Reports') unless Dir.exist?('CRA_Reports')
Dir.mkdir('logs') unless Dir.exist?('logs')

def generate_human_report(wallet_id, year, summary)
  <<~REPORT
    ==============================================
    TON TRANSACTION TAX REPORT - #{year}
    ==============================================
    Wallet: #{wallet_id}
    Report Generated: #{Time.now.strftime('%Y-%m-%d %H:%M:%S')}

    ----------------------------
    TRANSACTION SUMMARY
    ----------------------------
    Total Transactions: #{summary[:total].to_s.rjust(12)}
    Incoming TON:      #{summary[:incoming].to_s.rjust(12)}
    Outgoing TON:      #{summary[:outgoing].to_s.rjust(12)}
    Fees Paid:         #{summary[:fees].to_s.rjust(12)}
    Net Movement:      #{(summary[:incoming] - summary[:outgoing]).round(9).to_s.rjust(12)}

    Large Transactions (≥1 TON): #{summary[:large_tx].to_s.rjust(6)}
    ==============================================
  REPORT
end

# Process all wallets
Dir.glob('CSV_Files/ton_transactions_*.csv').group_by { |f|
  File.basename(f).split('_')[2..-3].join('_') rescue 'unknown'
}.each do |wallet_id, files|
  # Get most recent file
  latest_file = files.max_by { |f|
    timestamp = File.basename(f).split('_').last.gsub('.csv','')
    Time.strptime(timestamp, "%Y%m%d_%H%M%S") rescue Time.at(0)
  }

  puts "\n#{'=' * 50}"
  puts "Processing: #{wallet_id}".center(50)
  puts "File: #{File.basename(latest_file)}".center(50)
  puts '=' * 50

  # Process by year
  CSV.read(latest_file, headers: true).group_by { |tx|
    (Time.parse(tx['Date (UTC)']) rescue Time.now).year
  }.each do |year, transactions|
    summary = {
      total: transactions.size,
      incoming: transactions.sum { |t| t['Type'] == 'IN' ? t['Amount (TON)'].to_f : 0 }.round(9),
      outgoing: transactions.sum { |t| t['Type'] == 'OUT' ? t['Amount (TON)'].to_f : 0 }.round(9),
      fees: transactions.sum { |t| t['Fee (TON)'].to_f }.round(9),
      large_tx: transactions.count { |t| t['Amount (TON)'].to_f >= 1.0 }
    }

    # Generate pure CSV report
    CSV.open("CRA_Reports/cra_#{wallet_id}_#{year}.csv", 'w') do |csv|
      csv << ['wallet_id', 'year', 'total_txs', 'incoming_ton', 'outgoing_ton', 'fees_ton', 'net_movement', 'large_txs']
      csv << [
        wallet_id, year, summary[:total], summary[:incoming],
        summary[:outgoing], summary[:fees],
        (summary[:incoming] - summary[:outgoing]).round(9),
        summary[:large_tx]
      ]
    end

    # Generate human output
    human_report = generate_human_report(wallet_id, year, summary)
    puts human_report

    # Log human-readable version
    File.write("logs/report_#{wallet_id}_#{year}.txt", human_report)

    puts "↳ Saved machine-readable CSV and log file"
  end
end

puts "\n#{'=' * 50}"
puts "PROCESSING COMPLETE".center(50)
puts "#{Dir.glob('CRA_Reports/*.csv').size} reports generated".center(50)
puts "#{Dir.glob('logs/*.txt').size} logs created".center(50)
puts '=' * 50
