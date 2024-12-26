#include "twse_tick.hpp"
#include <iostream>

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
        // Option 1: Dump to stdout
        for (const auto &rec : odr_records)
        {
            nlohmann::json j = orderToJson(rec);
            std::cout << j.dump() << "\n";
            // or j.dump(2) for pretty printing, but typically NDJSON is single line
        }
        // Option 2: Dump to a file
        std::ofstream ofile("order/odr_output.jsonl"); // JSON Lines / NDJSON style
        for (const auto &rec : odr_records)
        {
            nlohmann::json j = orderToJson(rec);
            ofile << j.dump() << "\n";
        }
        ofile.close();

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
        std::ofstream dsp_ofile("snapshot/dsp_output.jsonl");
        for (const auto &snap : dsp_records)
        {
            nlohmann::json j = snapshotToJson(snap);
            dsp_ofile << j.dump() << "\n";
        }
        dsp_ofile.close();

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
        std::ofstream mth_ofile("transaction/mth_output.jsonl");
        for (const auto &tx : mth_records)
        {
            nlohmann::json j = transactionToJson(tx);
            mth_ofile << j.dump() << "\n";
        }
        mth_ofile.close();
    }
    catch (const std::exception &ex)
    {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}