use std::fs::File;
use std::io::{BufRead, BufReader, Error};

/// The "new" 190-byte snapshot format (after 2020/03/01).
/// Byte offsets (1-indexed as per your doc):
///   1-6   => securities_code
///   7-18  => display_time
///   19    => remark
///   20    => trend_flag
///   21    => match_flag
///   22    => trade_upper_lower_limit
///   23-28 => trade_price
///   29-36 => transaction_volume
///   37    => buy_tick_size
///   38    => buy_upper_lower_limit
///   39-108  => buy_5_price_volume
///   109    => sell_tick_size
///   110    => sell_upper_lower_limit
///   111-180 => sell_5_price_volume
///   181-188 => display_date
///   189-190 => match_staff
#[allow(dead_code)]
#[derive(Debug)]
struct TwseSnapshotNew {
    securities_code: String,
    display_time: String,
    remark: char,
    trend_flag: char,
    match_flag: char,
    trade_upper_lower_limit: char,
    trade_price: String,
    transaction_volume: String,
    buy_tick_size: char,
    buy_upper_lower_limit: char,
    buy_5_price_volume: String,
    sell_tick_size: char,
    sell_upper_lower_limit: char,
    sell_5_price_volume: String,
    display_date: String,
    match_staff: String,
}

/// The "old" 186-byte snapshot format (before 2020/03/01).
/// Byte offsets (1-indexed):
///   1-6   => securities_code
///   7-14  => display_time
///   15    => remark
///   16    => trend_flag
///   17    => match_flag
///   18    => trade_upper_lower_limit
///   19-24 => trade_price
///   25-32 => transaction_volume
///   33    => buy_tick_size
///   34    => buy_upper_lower_limit
///   35-104  => buy_5_price_volume
///   105    => sell_tick_size
///   106    => sell_upper_lower_limit
///   107-176 => sell_5_price_volume
///   177-184 => display_date
///   185-186 => match_staff
#[allow(dead_code)]
#[derive(Debug)]
struct TwseSnapshotOld {
    securities_code: String,
    display_time: String,
    remark: char,
    trend_flag: char,
    match_flag: char,
    trade_upper_lower_limit: char,
    trade_price: String,
    transaction_volume: String,
    buy_tick_size: char,
    buy_upper_lower_limit: char,
    buy_5_price_volume: String,
    sell_tick_size: char,
    sell_upper_lower_limit: char,
    sell_5_price_volume: String,
    display_date: String,
    match_staff: String,
}

/// Parse a 190-byte line into TwseSnapshotNew
fn parse_new_format(line: &str) -> TwseSnapshotNew {
    TwseSnapshotNew {
        securities_code:         line[0..6].trim().to_string(),
        display_time:            line[6..18].trim().to_string(),
        remark:                  line.chars().nth(18).unwrap_or(' '),
        trend_flag:              line.chars().nth(19).unwrap_or(' '),
        match_flag:              line.chars().nth(20).unwrap_or(' '),
        trade_upper_lower_limit: line.chars().nth(21).unwrap_or(' '),
        trade_price:             line[22..28].trim().to_string(),
        transaction_volume:      line[28..36].trim().to_string(),
        buy_tick_size:           line.chars().nth(36).unwrap_or(' '),
        buy_upper_lower_limit:   line.chars().nth(37).unwrap_or(' '),
        buy_5_price_volume:      line[38..108].trim().to_string(),
        sell_tick_size:          line.chars().nth(108).unwrap_or(' '),
        sell_upper_lower_limit:  line.chars().nth(109).unwrap_or(' '),
        sell_5_price_volume:     line[110..180].trim().to_string(),
        display_date:            line[180..188].trim().to_string(),
        match_staff:             line[188..190].trim().to_string(),
    }
}

/// Parse a 186-byte line into TwseSnapshotOld
fn parse_old_format(line: &str) -> TwseSnapshotOld {
    TwseSnapshotOld {
        securities_code:         line[0..6].trim().to_string(),
        display_time:            line[6..14].trim().to_string(),
        remark:                  line.chars().nth(14).unwrap_or(' '),
        trend_flag:              line.chars().nth(15).unwrap_or(' '),
        match_flag:              line.chars().nth(16).unwrap_or(' '),
        trade_upper_lower_limit: line.chars().nth(17).unwrap_or(' '),
        trade_price:             line[18..24].trim().to_string(),
        transaction_volume:      line[24..32].trim().to_string(),
        buy_tick_size:           line.chars().nth(32).unwrap_or(' '),
        buy_upper_lower_limit:   line.chars().nth(33).unwrap_or(' '),
        buy_5_price_volume:      line[34..104].trim().to_string(),
        sell_tick_size:          line.chars().nth(104).unwrap_or(' '),
        sell_upper_lower_limit:  line.chars().nth(105).unwrap_or(' '),
        sell_5_price_volume:     line[106..176].trim().to_string(),
        display_date:            line[176..184].trim().to_string(),
        match_staff:             line[184..186].trim().to_string(),
    }
}

fn main() -> Result<(), Error> {
    // Open the file (replace with your actual file path).
    // Each line in the file should be either 190 or 186 characters (no trailing newline).
    let file = File::open("snapshot/Sample")?;
    // let mut file = File::open("snapshot/Sample_new")?;
    let reader = BufReader::new(file);

    for (i, line_result) in reader.lines().enumerate() {
        let line_raw = line_result?;
        // Trim end to remove any trailing newline or carriage return
        let line = line_raw.trim_end();

        if line.is_empty() {
            // Possibly skip empty lines
            continue;
        }

        match line.len() {
            190 => {
                // Parse new format
                let snapshot = parse_new_format(line);
                println!("Line {} => New format => {:?}", i + 1, snapshot);
            },
            186 => {
                // Parse old format
                let snapshot = parse_old_format(line);
                println!("Line {} => Old format => {:?}", i + 1, snapshot);
            },
            other => {
                eprintln!(
                    "Line {} => Unexpected length {}. Skipping: {}",
                    i + 1,
                    other,
                    line
                );
            }
        }
    }

    Ok(())
}