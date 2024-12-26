#include "twse_tick.hpp"
#include <iostream>

static std::string buySellToString(BuySell bs)
{
    switch (bs)
    {
    case BuySell::Buy:
        return "BUY";
    case BuySell::Sell:
        return "SELL";
    default:
        return "UNKNOWN";
    }
}

static std::string matchFlagToString(MatchFlag mf)
{
    switch (mf)
    {
    case MatchFlag::NoMatch:
        return "NO_MATCH";
    case MatchFlag::Matched:
        return "MATCHED";
    case MatchFlag::Stabilize:
        return "STABILIZE";
    }
    return "NO_MATCH";
}

int main()
{
    try
    {
        // 1. Load ODR
        auto odr_records = loadOdrFile("order/odr");
        std::cout << "Loaded " << odr_records.size() << " ODR records.\n";
        if (!odr_records.empty())
        {
            auto &r = odr_records[0];
            std::cout << "First ODR record:\n"
                      << "  order_date=" << r.order_date << "\n"
                      << "  securities_code=" << r.securities_code << "\n"
                      << "  buy_sell=" << buySellToString(r.buy_sell) << "\n"
                      << "  order_price=" << r.order_price << "\n"
                      << "  changed_trade_volume=" << r.changed_trade_volume << "\n"
                      << std::endl;
        }

        // 2. Load DSP
        auto dsp_records = loadDspFile("snapshot/Sample");
        std::cout << "Loaded " << dsp_records.size() << " DSP records.\n";
        if (!dsp_records.empty())
        {
            auto &s = dsp_records[0];
            std::cout << "First DSP record:\n"
                      << "  securities_code=" << s.securities_code << "\n"
                      << "  display_time=" << s.display_time << "\n"
                      << "  match_flag=" << matchFlagToString(s.match_flag) << "\n"
                      << "  trade_price=" << s.trade_price << "\n"
                      << "  transaction_volume=" << s.transaction_volume << "\n"
                      << std::endl;
        }

        // 3. Load MTH
        auto mth_records = loadMthFile("transaction/mth");
        std::cout << "Loaded " << mth_records.size() << " MTH records.\n";
        if (!mth_records.empty())
        {
            auto &t = mth_records[0];
            std::cout << "First MTH record:\n"
                      << "  trade_date=" << t.trade_date << "\n"
                      << "  securities_code=" << t.securities_code << "\n"
                      << "  buy_sell=" << buySellToString(t.buy_sell) << "\n"
                      << "  trade_price=" << t.trade_price << "\n"
                      << "  trade_volume=" << t.trade_volume << "\n"
                      << std::endl;
        }
    }
    catch (const std::exception &ex)
    {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}