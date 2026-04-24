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

eval:
	$(CXX) $(CXXFLAGS) eval/eval_suite.cpp src/column_store.cpp src/csv_parser.cpp src/query_engine.cpp src/output_writer.cpp src/column_file_io.cpp -o eval_runner
	@echo "Eval build successful: eval_runner"
	./eval_runner ../data/ResalePricesSingapore.csv U2220136J 5

ifeq ($(OS),Windows_NT)
clean:
	-@if exist main.o del /Q main.o
	-@if exist src\column_store.o del /Q src\column_store.o
	-@if exist src\csv_parser.o del /Q src\csv_parser.o
	-@if exist src\query_engine.o del /Q src\query_engine.o
	-@if exist src\output_writer.o del /Q src\output_writer.o
	-@if exist src\column_file_io.o del /Q src\column_file_io.o
	-@if exist column_store.exe del /Q column_store.exe
	-@if exist column_store del /Q column_store
	-@if exist test_runner.exe del /Q test_runner.exe
	-@if exist test_runner del /Q test_runner
	-@if exist eval_runner.exe del /Q eval_runner.exe
	-@if exist eval_runner del /Q eval_runner
	@echo "Cleaned."
else
clean:
	rm -f main.o src/column_store.o src/csv_parser.o src/query_engine.o src/output_writer.o src/column_file_io.o column_store test_runner eval_runner
	@echo "Cleaned."
endif
