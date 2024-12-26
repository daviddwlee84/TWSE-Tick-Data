#include "twse_tick.hpp"
#include <iostream>

// g++ -std=c++17 main.cpp -o twse_parser

int main()
{
    try
    {
        // Let's assume we have an ODR file
        std::string odr_file = "order/odr";
        std::vector<TwseOrderBook> order_records = loadOdrFile(odr_file);

        std::cout << "Loaded " << order_records.size()
                  << " ODR records from: " << odr_file << "\n";

        if (!order_records.empty())
        {
            const auto &rec = order_records[0];
            std::cout << "First record sample:\n"
                      << "order_date = " << rec.order_date << "\n"
                      << "securities_code = " << rec.securities_code << "\n"
                      << "buy_sell = "
                      << ((rec.buy_sell == BuySell::Buy) ? "BUY" : (rec.buy_sell == BuySell::Sell) ? "SELL"
                                                                                                   : "UNKNOWN")
                      << "\n"
                      << "trade_type_code = " << rec.trade_type_code << "\n"
                      << "order_time = " << rec.order_time << "\n"
                      << "order_price = " << rec.order_price << "\n"
                      << "changed_trade_volume = " << rec.changed_trade_volume << "\n"
                      << "type_of_investor = " << rec.type_of_investor << "\n"
                      << std::endl;
        }
    }
    catch (const std::exception &ex)
    {
        std::cerr << "Exception: " << ex.what() << std::endl;
    }
    return 0;
}