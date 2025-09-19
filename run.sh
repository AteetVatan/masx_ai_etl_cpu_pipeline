#!/bin/bash
# Run script for MASX AI ETL CPU Pipeline
# Provides a convenient way to start the FastAPI server

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed or not in PATH"
    exit 1
fi

# Check Python version
python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
required_version="3.12"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    print_error "Python 3.12+ is required. Current version: $python_version"
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    print_warning "Virtual environment not found. Creating one..."
    python3 -m venv venv
    print_success "Virtual environment created"
fi

# Activate virtual environment
print_info "Activating virtual environment..."
source venv/bin/activate

# Check if requirements are installed
if [ ! -f "venv/pyvenv.cfg" ] || [ ! -d "venv/lib" ]; then
    print_warning "Virtual environment appears to be corrupted. Recreating..."
    rm -rf venv
    python3 -m venv venv
    source venv/bin/activate
fi

# Install requirements if needed
if [ ! -f "venv/.requirements_installed" ]; then
    print_info "Installing requirements..."
    pip install --upgrade pip
    pip install -r requirements.txt
    
    # Download spaCy models
    print_info "Downloading spaCy models..."
    python -m spacy download en_core_web_sm
    python -m spacy download es_core_news_sm
    python -m spacy download fr_core_news_sm
    python -m spacy download de_core_news_sm
    
    # Mark requirements as installed
    touch venv/.requirements_installed
    print_success "Requirements installed"
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    if [ -f "env.example" ]; then
        print_warning ".env file not found. Copying from env.example..."
        cp env.example .env
        print_warning "Please edit .env file with your configuration before running again"
        exit 1
    else
        print_error ".env file not found and env.example is not available"
        exit 1
    fi
fi

# Check if .env file has required variables
if ! grep -q "SUPABASE_URL=" .env || ! grep -q "SUPABASE_KEY=" .env; then
    print_warning ".env file exists but may not be properly configured"
    print_info "Please ensure SUPABASE_URL and SUPABASE_KEY are set in .env"
fi

# Run the application
print_info "Starting MASX AI ETL CPU Pipeline..."
python run.py
