#include <iostream>
#include <boost/asio.hpp>
#include <gtest/gtest.h>

using namespace std;
using boost::asio::ip::tcp;

int main(int argc, char **argv)
{
    try {
        boost::asio::io_service io_service;
        tcp::resolver resolver(io_service);
        tcp::resolver::query query(tcp::v4(), "localhost", "9160");
        tcp::resolver::iterator iterator= resolver.resolve(query);
        tcp::socket socket(io_service);
        socket.connect(*iterator);
    } catch (exception &) {
        cerr << "Cassandra must be running on localhost:9160" << endl;
	    return EXIT_FAILURE;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
