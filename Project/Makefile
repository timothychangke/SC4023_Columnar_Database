.PHONY: run test eval clean

CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -Iinclude

TARGET   := column_store

SRCS     := main.cpp src/column_store.cpp src/csv_parser.cpp src/query_engine.cpp src/output_writer.cpp src/column_file_io.cpp

OBJS     := $(SRCS:.cpp=.o)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^
	@echo "Build successful: $(TARGET)"

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

run: $(TARGET)
	./$(TARGET) U2220136J

test:
	$(CXX) $(CXXFLAGS) eval/eval_suite.cpp src/column_store.cpp src/csv_parser.cpp src/query_engine.cpp src/output_writer.cpp src/column_file_io.cpp -o test_runner
	@echo "Test build successful: test_runner"
	./test_runner ../data/ResalePricesSingapore.csv U2220136J 5

eval:
	$(CXX) $(CXXFLAGS) eval/eval_suite.cpp src/column_store.cpp src/csv_parser.cpp src/query_engine.cpp src/output_writer.cpp src/column_file_io.cpp -o eval_runner
	@echo "Eval build successful: eval_runner"
	./eval_runner ../data/ResalePricesSingapore.csv U2220136J 5

clean:
	rm -f main.o src/column_store.o src/csv_parser.o src/query_engine.o src/output_writer.o src/column_file_io.o column_store test_runner eval_runner
	@echo "Cleaned."
