CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra

# If installed system-wide, might not need extra -I
# If "json.hpp" is in local dir, do:
CXXFLAGS += -Iinclude

TARGET = twse_parser

SOURCES = main.cpp
HEADERS = twse_tick.hpp
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: %.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $<

clean:
	rm -f $(OBJECTS) $(TARGET)