# =============================================================================
# Makefile — CE/CZ4123/SC4023 Big Data Management Project
# =============================================================================
# Usage:
#   make          — build the executable (default target)
#   make clean    — remove compiled objects and the executable
#   make run      — build and run with a default matric number (for quick testing)
# =============================================================================

CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -Iinclude

# Output executable name
TARGET   := column_store

# Source files (all .cpp in src/ plus main.cpp at project root)
SRCS     := main.cpp \
            src/column_store.cpp \
            src/csv_parser.cpp   \
            src/query_engine.cpp \
            src/output_writer.cpp

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
		-o test_runner
	@echo "Test build successful: test_runner"
	./test_runner

# Remove build artifacts
clean:
	rm -f $(OBJS) $(TARGET) test_runner
	@echo "Cleaned."

.PHONY: run clean test