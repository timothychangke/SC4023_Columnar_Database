# =============================================================================
# Makefile — CE/CZ4123/SC4023 Big Data Management Project
# =============================================================================
# Usage:
#   make          — build the executable (default target)
#   make clean    — remove compiled objects and the executable
#   make run      — build and run with a default matric number (for quick testing)
# =============================================================================

.PHONY: run test eval clean

CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -Iinclude

ifeq ($(OS),Windows_NT)
CLEAN_CMD := del /Q /F main.o src\column_store.o src\csv_parser.o src\query_engine.o src\output_writer.o src\column_file_io.o column_store.exe column_store test_runner.exe test_runner eval_runner.exe eval_runner 2>nul
else
CLEAN_CMD := rm -f $(OBJS) $(TARGET) test_runner eval_runner
endif

# Output executable name
TARGET   := column_store

# Source files (all .cpp in src/ plus main.cpp at project root)
SRCS     := main.cpp \
            src/column_store.cpp \
            src/csv_parser.cpp   \
            src/query_engine.cpp \
            src/output_writer.cpp \
            src/column_file_io.cpp

# Derive object file names (placed alongside their sources)
OBJS     := $(SRCS:.cpp=.o)

# Default target: build the executable
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^
	@echo "Build successful: $(TARGET)"

# Pattern rule: compile each .cpp to a .o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

# Quick test run — replace with your actual matric number
run: $(TARGET)
	./$(TARGET) A5656567B

# Build and run unit tests
test:
	$(CXX) $(CXXFLAGS) \
		tests/test_suite.cpp \
		src/column_store.cpp \
		src/csv_parser.cpp \
		src/query_engine.cpp \
		src/output_writer.cpp \
		src/column_file_io.cpp \
		-o test_runner
	@echo "Test build successful: test_runner"
	./test_runner

# Build and run evaluation suite
eval:
	$(CXX) $(CXXFLAGS) \
		eval/eval_suite.cpp \
		src/column_store.cpp \
		src/csv_parser.cpp \
		src/query_engine.cpp \
		src/output_writer.cpp \
		src/column_file_io.cpp \
		-o eval_runner
	@echo "Eval build successful: eval_runner"
	./eval_runner ../data/ResalePricesSingapore.csv A5656567B 5

# Remove build artifacts
clean:
	$(CLEAN_CMD)
	@echo "Cleaned."