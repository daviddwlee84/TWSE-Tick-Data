#ifndef TWSE_TICK_HPP
#define TWSE_TICK_HPP

#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <cstdlib> // for std::atoi, std::strtof
#include <cctype>

//------------------------------------------------------------------------------
// 1. Enums and Helper Parsers
//------------------------------------------------------------------------------

enum class BuySell
{
    Buy,
    Sell,
    Unknown
};

inline BuySell parseBuySell(const std::string &bs)
{
    if (bs == "B")
        return BuySell::Buy;
    if (bs == "S")
        return BuySell::Sell;
    return BuySell::Unknown;
}

// Parse strings like "+0000001000" or "-0000000500"
inline int parseSignAndInt(const std::string &raw)
{
    if (raw.empty())
        return 0;
    // raw[0] is '+' or '-'
    char sign_char = raw[0];
    std::string numeric_part = raw.substr(1); // skip sign
    int magnitude = std::atoi(numeric_part.c_str());
    return (sign_char == '-') ? -magnitude : magnitude;
}

// Parse float (e.g. "0078.35" -> 78.35)
inline float parseFloat(const std::string &raw)
{
    return std::strtof(raw.c_str(), nullptr);
}

// Parse small numeric code like "0", "1", "2" -> int
inline int parseCode(const std::string &raw)
{
    return std::atoi(raw.c_str());
}

//------------------------------------------------------------------------------
// 2. Data Structures
//
// 2.1 Order Book (ODR) - TwseOrderBook
//------------------------------------------------------------------------------

struct TwseOrderBook
{
    // keep these as raw strings (no date/time parse)
    std::string order_date;      // positions [0..7]
    std::string securities_code; // [8..13]

    BuySell buy_sell; // [14]

    int trade_type_code;    // [15] (0=normal,1=block,2=odd-lot)
    std::string order_time; // [16..23]

    std::string order_number_ii; // [24..28]

    int changed_trade_code;   // [29] (1..6)
    float order_price;        // [30..36]
    int changed_trade_volume; // [37..47]

    int order_type_code;                 // [48]
    std::string notes_investors_channel; // [49]
    std::string order_report_print;      // [50..53]
    std::string type_of_investor;        // [54]
    std::string order_number_i;          // [55..58]
};

//------------------------------------------------------------------------------
// 2.2 Snapshot (DSP) - TwseSnapshot
//------------------------------------------------------------------------------

enum class MatchFlag
{
    NoMatch,  // ' '
    Matched,  // 'Y'
    Stabilize // 'S'
    // you could add more if needed
};

inline MatchFlag parseMatchFlag(const std::string &mf)
{
    if (mf == " ")
        return MatchFlag::NoMatch;
    if (mf == "Y")
        return MatchFlag::Matched;
    if (mf == "S")
        return MatchFlag::Stabilize;
    return MatchFlag::NoMatch;
}

struct TwseSnapshot
{
    // raw strings we do not parse to date/time => see note
    // If you prefer to keep them strings, no problem.
    // or you can parse time into a custom struct
    std::string securities_code; // [0..5]
    std::string display_time;    // [6..13]

    // small-coded fields
    std::string remark;     // [14] (" ", "T", "S", "A")
    std::string trend_flag; // [15] (" ", "R", "F")

    MatchFlag match_flag;          // [16] (" ", "Y", "S")
    std::string trade_upper_lower; // [17] (" ", "R", "F")

    float trade_price;      // [18..23] -> parseFloat(6 chars)
    int transaction_volume; // [24..31] -> parse as int

    int buy_tick_size;                 // [32] -> parse single digit
    std::string buy_upper_lower_limit; // [33] -> " ", "R", "F"

    // 70 chars
    std::string buy_5_price_volume; // [34..103]

    int sell_tick_size;                 // [104]
    std::string sell_upper_lower_limit; // [105]
    std::string sell_5_price_volume;    // [106..175]

    std::string display_date; // [176..183]
    std::string match_staff;  // [184..185]
};

//------------------------------------------------------------------------------
// 2.3 Transaction (MTH) - TwseTransaction
//------------------------------------------------------------------------------

struct TwseTransaction
{
    std::string trade_date;      // [0..7]
    std::string securities_code; // [8..13]
    BuySell buy_sell;            // [14]
    int trade_type_code;         // [15]

    std::string trade_time;      // [16..23]
    std::string trade_number;    // [24..31]
    std::string order_number_ii; // [32..36]

    float trade_price; // [37..43]
    int trade_volume;  // [44..52]

    std::string trading_report; // [53..56]

    int order_type_code;          // [57]
    std::string type_of_investor; // [58]
    std::string order_number_i;   // [59..62]
};

//------------------------------------------------------------------------------
// 3. Parse Functions
//------------------------------------------------------------------------------

inline TwseOrderBook parseOrderLine(const std::string &line)
{
    if (line.size() < 59)
    {
        throw std::runtime_error("Line too short (ODR requires 59 chars).");
    }
    TwseOrderBook rec;

    rec.order_date = line.substr(0, 8);                              // [0..7]
    rec.securities_code = line.substr(8, 6);                         // [8..13]
    rec.buy_sell = parseBuySell(line.substr(14, 1));                 // [14]
    rec.trade_type_code = parseCode(line.substr(15, 1));             // [15]
    rec.order_time = line.substr(16, 8);                             // [16..23]
    rec.order_number_ii = line.substr(24, 5);                        // [24..28]
    rec.changed_trade_code = parseCode(line.substr(29, 1));          // [29]
    rec.order_price = parseFloat(line.substr(30, 7));                // [30..36]
    rec.changed_trade_volume = parseSignAndInt(line.substr(37, 11)); // [37..47]
    rec.order_type_code = parseCode(line.substr(48, 1));             // [48]
    rec.notes_investors_channel = line.substr(49, 1);                // [49]
    rec.order_report_print = line.substr(50, 4);                     // [50..53]
    rec.type_of_investor = line.substr(54, 1);                       // [54]
    rec.order_number_i = line.substr(55, 4);                         // [55..58]

    return rec;
}

inline TwseSnapshot parseSnapshotLine(const std::string &line)
{
    if (line.size() < 186)
    {
        throw std::runtime_error("Line too short (DSP requires 186 chars).");
    }
    TwseSnapshot snap;

    snap.securities_code = line.substr(0, 6);             // [0..5]
    snap.display_time = line.substr(6, 8);                // [6..13]
    snap.remark = line.substr(14, 1);                     // [14]
    snap.trend_flag = line.substr(15, 1);                 // [15]
    snap.match_flag = parseMatchFlag(line.substr(16, 1)); // [16]
    snap.trade_upper_lower = line.substr(17, 1);          // [17]

    snap.trade_price = parseFloat(line.substr(18, 6));               // [18..23]
    snap.transaction_volume = std::atoi(line.substr(24, 8).c_str()); // [24..31]

    snap.buy_tick_size = parseCode(line.substr(32, 1)); // [32]
    snap.buy_upper_lower_limit = line.substr(33, 1);    // [33]

    snap.buy_5_price_volume = line.substr(34, 70); // [34..103]

    snap.sell_tick_size = parseCode(line.substr(104, 1)); // [104]
    snap.sell_upper_lower_limit = line.substr(105, 1);    // [105]
    snap.sell_5_price_volume = line.substr(106, 70);      // [106..175]

    snap.display_date = line.substr(176, 8); // [176..183]
    snap.match_staff = line.substr(184, 2);  // [184..185]

    return snap;
}

inline TwseTransaction parseTransactionLine(const std::string &line)
{
    if (line.size() < 63)
    {
        throw std::runtime_error("Line too short (MTH requires 63 chars).");
    }
    TwseTransaction tx;

    tx.trade_date = line.substr(0, 8);                  // [0..7]
    tx.securities_code = line.substr(8, 6);             // [8..13]
    tx.buy_sell = parseBuySell(line.substr(14, 1));     // [14]
    tx.trade_type_code = parseCode(line.substr(15, 1)); // [15]

    tx.trade_time = line.substr(16, 8);      // [16..23]
    tx.trade_number = line.substr(24, 8);    // [24..31]
    tx.order_number_ii = line.substr(32, 5); // [32..36]

    tx.trade_price = parseFloat(line.substr(37, 7));         // [37..43]
    tx.trade_volume = std::atoi(line.substr(44, 9).c_str()); // [44..52]

    tx.trading_report = line.substr(53, 4);             // [53..56]
    tx.order_type_code = parseCode(line.substr(57, 1)); // [57]
    tx.type_of_investor = line.substr(58, 1);           // [58]
    tx.order_number_i = line.substr(59, 4);             // [59..62]

    return tx;
}

//------------------------------------------------------------------------------
// 4. Loading each file
//------------------------------------------------------------------------------

inline std::vector<TwseOrderBook> loadOdrFile(const std::string &filepath)
{
    std::vector<TwseOrderBook> records;
    std::ifstream fin(filepath, std::ios::binary);
    if (!fin.is_open())
    {
        throw std::runtime_error("Cannot open ODR file: " + filepath);
    }
    std::string line;
    while (std::getline(fin, line))
    {
        if (line.size() == 59)
        {
            TwseOrderBook rec = parseOrderLine(line);
            records.push_back(rec);
        }
    }
    return records;
}

inline std::vector<TwseSnapshot> loadDspFile(const std::string &filepath)
{
    std::vector<TwseSnapshot> records;
    std::ifstream fin(filepath, std::ios::binary);
    if (!fin.is_open())
    {
        throw std::runtime_error("Cannot open DSP file: " + filepath);
    }
    std::string line;
    while (std::getline(fin, line))
    {
        if (line.size() == 186)
        {
            TwseSnapshot snap = parseSnapshotLine(line);
            records.push_back(snap);
        }
    }
    return records;
}

inline std::vector<TwseTransaction> loadMthFile(const std::string &filepath)
{
    std::vector<TwseTransaction> records;
    std::ifstream fin(filepath, std::ios::binary);
    if (!fin.is_open())
    {
        throw std::runtime_error("Cannot open MTH file: " + filepath);
    }
    std::string line;
    while (std::getline(fin, line))
    {
        if (line.size() == 63)
        {
            TwseTransaction tx = parseTransactionLine(line);
            records.push_back(tx);
        }
    }
    return records;
}

#endif // TWSE_TICK_HPP