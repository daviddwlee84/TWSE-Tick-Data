# Simple Makefile

CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra

# The main executable
TARGET = twse_parser

# Source files
SOURCES = main.cpp
HEADERS = twse_tick.hpp

# Object files
OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: %.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -c $<

clean:
	rm -f $(OBJECTS) $(TARGET)