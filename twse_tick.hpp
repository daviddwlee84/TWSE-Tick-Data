#ifndef TWSE_TICK_HPP
#define TWSE_TICK_HPP

#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <cctype>  // for std::isdigit
#include <cstdlib> // for std::strtof, std::strtol
#include <cmath>   // for float/double

//
// 1. Some helper enum or functions to parse fields
//

// 1.1 Buy/Sell enum
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

// 1.2 parseSignAndInt => e.g. "+0000001000" => 1000, "-0000000500" => -500
inline int parseSignAndInt(const std::string &raw)
{
    if (raw.empty())
        return 0;
    char sign_char = raw[0];
    std::string numeric_part = raw.substr(1);
    int magnitude = std::atoi(numeric_part.c_str());
    return (sign_char == '-') ? -magnitude : magnitude;
}

// 1.3 parseFloat => either direct float with potential decimal (e.g. "0078.35")
//    or parse a 6-digit numeric as a 2-decimal float if you prefer.
inline float parseFloat(const std::string &raw)
{
    // Here we assume the string may have leading zeros or decimal points
    // e.g. "0078.35" => 78.35
    // We use std::strtof or std::stof
    return std::strtof(raw.c_str(), nullptr);
}

//
// 2. Example: TwseOrderBook with more "usable" types
//
//    - We keep date/time as string
//    - We parse buy_sell into an enum
//    - We parse order_price into float
//    - We parse changed_trade_volume (sign + int) into int
//

struct TwseOrderBook
{
    // Keep these as strings
    std::string order_date;      // (no date/time parse)
    std::string securities_code; // e.g. "0050  "

    // Parse into an enum
    BuySell buy_sell; // B=Buy, S=Sell

    // If you have "0","1","2" and want an enum, do similarly
    // Here we'll just store as int or as a small enum
    int trade_type_code; // 0=normal,1=block,2=odd-lot?

    // Keep time as string
    std::string order_time;

    // More strings
    std::string order_number_ii;
    // changed_trade_code -> we can store as int or an enum
    // But let's store as int for demonstration
    int changed_trade_code;

    // parse float
    float order_price;

    // parse sign+int
    int changed_trade_volume;

    // keep as string or small int
    int order_type_code;
    std::string notes_investors_channel;
    std::string order_report_print;
    std::string type_of_investor;
    std::string order_number_i;
};

//
// 3. Parsing an ODR line with "usable" fields
//
inline TwseOrderBook parseOrderLine(const std::string &line)
{
    if (line.size() < 59)
    {
        throw std::runtime_error("Line too short for ODR (needs 59 chars).");
    }

    TwseOrderBook rec;

    // 1-8
    rec.order_date = line.substr(0, 8);
    // 9-14
    rec.securities_code = line.substr(8, 6);

    // 15 => 'B' or 'S'
    {
        std::string bs = line.substr(14, 1);
        rec.buy_sell = parseBuySell(bs);
    }

    // 16 => trade_type_code (0,1,2)
    {
        std::string ttc = line.substr(15, 1);
        rec.trade_type_code = std::atoi(ttc.c_str()); // or parse an enum
    }

    // 17-24 => no parse => string
    rec.order_time = line.substr(16, 8);

    // 25-29 => string
    rec.order_number_ii = line.substr(24, 5);

    // 30 => changed_trade_code => store as int
    {
        std::string ctc = line.substr(29, 1);
        rec.changed_trade_code = std::atoi(ctc.c_str());
    }

    // 31-37 => order_price => float
    {
        std::string pr = line.substr(30, 7);
        rec.order_price = parseFloat(pr);
    }

    // 38-48 => changed_trade_volume => sign+int
    {
        std::string ctv = line.substr(37, 11);
        rec.changed_trade_volume = parseSignAndInt(ctv);
    }

    // 49 => order_type_code => store as int
    {
        std::string otc = line.substr(48, 1);
        rec.order_type_code = std::atoi(otc.c_str());
    }

    // 50 => string
    rec.notes_investors_channel = line.substr(49, 1);

    // 51-54 => string
    rec.order_report_print = line.substr(50, 4);

    // 55 => string
    rec.type_of_investor = line.substr(54, 1);

    // 56-59 => string
    rec.order_number_i = line.substr(55, 4);

    return rec;
}

//
// 4. Example load file for ODR
//
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
        // If exactly 59 chars, parse
        if (line.size() == 59)
        {
            TwseOrderBook rec = parseOrderLine(line);
            records.push_back(rec);
        }
    }
    return records;
}

#endif // TWSE_TICK_HPP