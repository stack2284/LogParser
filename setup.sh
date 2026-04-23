#!/bin/bash
# HSHL Log Parser - Automated Setup & Run Script

echo "==========================================================="
echo "⚡ HSHL LOG PARSER - ENVIRONMENT SETUP & COMPILATION ⚡"
echo "==========================================================="

echo -e "\n[1/3] Installing Python Dependencies..."
python3 -m pip install --upgrade pip
python3 -m pip install streamlit plotly pandas datasketch scikit-learn numpy prometheus-client pybind11

echo -e "\n[2/3] Compiling Native C++ Parsing Engine..."
# Remove any old compiled binaries to ensure a clean build
rm -f fast_log_parser*.so

# Dynamically fetch the correct active python headers
EXT_SUFFIX=$(python3 -c "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))")
PYBIND_INCLUDES=$(python3 -m pybind11 --includes)

# Compile using Clang with OpenMP and Native Optimizations
c++ -O3 -march=native -flto -DNDEBUG -std=c++17 -shared -fPIC \
  -Xpreprocessor -fopenmp \
  -I/opt/homebrew/include \
  -I/opt/homebrew/opt/libomp/include \
  ${PYBIND_INCLUDES} \
  parser_module.cpp \
  -L/opt/homebrew/lib -lre2 \
  -L/opt/homebrew/opt/libomp/lib -lomp \
  -o fast_log_parser${EXT_SUFFIX} \
  -undefined dynamic_lookup

if [ $? -eq 0 ]; then
    echo "C++ Engine successfully compiled!"
else
    echo "Compilation failed. Check your Homebrew or LLVM OpenMP installation."
    exit 1
fi

echo -e "\n[3/3] Launching Presentation Dashboard..."
echo "Starting locally. Your browser should open automatically..."
streamlit run app.py
