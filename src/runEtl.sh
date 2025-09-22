#!/bin/bash

# ETL Pipeline Runner Script
# This script runs the ATP Order Stage pipeline for a date range
# Usage: ./runEtl.sh [START_DATE] [END_DATE]
# Example: ./runEtl.sh 2024-01-01 2024-01-31

# set -e  # Exit on any error - commented out to prevent premature exits

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to validate date format (YYYY-MM-DD)
validate_date() {
    local date_str=$1
    if [[ ! $date_str =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        print_error "Invalid date format: $date_str. Please use YYYY-MM-DD format."
        return 1
    fi
    
    # Check if date is valid using date command
    if ! date -d "$date_str" >/dev/null 2>&1; then
        print_error "Invalid date: $date_str"
        return 1
    fi
    
    return 0
}

# Function to get next date
get_next_date() {
    local current_date=$1
    date -d "$current_date + 1 day" +%Y-%m-%d
}

# Function to check if first date is before or equal to second date
is_date_before_or_equal() {
    local date1=$1
    local date2=$2
    [[ "$date1" < "$date2" ]] || [[ "$date1" == "$date2" ]]
}

# Function to run ETL for a single date
run_etl_for_date() {
    local target_date=$1
    local log_file="etl_${target_date}.log"
    
    print_info "Starting ETL for date: $target_date"
    print_info "Log file: $log_file"
    
    # Run the Python script with the target date
    if uv run ATP_Order_Stage_pipeline.py "$target_date" > "$log_file" 2>&1; then
        print_success "ETL completed successfully for date: $target_date"
        return 0
    else
        print_error "ETL failed for date: $target_date. Check log file: $log_file"
        return 1
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [START_DATE] [END_DATE]"
    echo ""
    echo "Arguments:"
    echo "  START_DATE    Start date in YYYY-MM-DD format (optional, defaults to yesterday)"
    echo "  END_DATE      End date in YYYY-MM-DD format (optional, defaults to START_DATE)"
    echo ""
    echo "Examples:"
    echo "  $0                           # Run for yesterday only"
    echo "  $0 2024-01-15               # Run for 2024-01-15 only"
    echo "  $0 2024-01-01 2024-01-31    # Run for entire January 2024"
    echo "  $0 2024-01-01 2024-01-01    # Run for single day 2024-01-01"
    echo ""
    echo "Notes:"
    echo "  - Dates should be in YYYY-MM-DD format"
    echo "  - START_DATE should be before or equal to END_DATE"
    echo "  - Log files will be created as etl_YYYY-MM-DD.log for each date"
    echo "  - The script will stop on first error unless you modify the error handling"
}

# Main script logic
main() {
    local start_date
    local end_date
    
    # Check if help is requested
    if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
        show_usage
        exit 0
    fi
    
    # Set default dates
    if [[ -z "$1" ]]; then
        # Default to yesterday if no start date provided
        start_date=$(date -d "yesterday" +%Y-%m-%d)
        print_info "No start date provided, using yesterday: $start_date"
    else
        start_date="$1"
    fi
    
    if [[ -z "$2" ]]; then
        # Default to start_date if no end date provided
        end_date="$start_date"
        print_info "No end date provided, using start date: $end_date"
    else
        end_date="$2"
    fi
    
    # Validate dates
    print_info "Validating dates..."
    if ! validate_date "$start_date"; then
        exit 1
    fi
    
    if ! validate_date "$end_date"; then
        exit 1
    fi
    
    # Check if start_date <= end_date
    if ! is_date_before_or_equal "$start_date" "$end_date"; then
        print_error "Start date ($start_date) must be before or equal to end date ($end_date)"
        exit 1
    fi
    
    # Check if Python script exists
    if [[ ! -f "ATP_Order_Stage_pipeline.py" ]]; then
        print_error "Python script 'ATP_Order_Stage_pipeline.py' not found in current directory"
        exit 1
    fi
    
    # Calculate total days
    local current_date="$start_date"
    local total_days=0
    while is_date_before_or_equal "$current_date" "$end_date"; do
        ((total_days++))
        current_date=$(get_next_date "$current_date")
    done
    
    print_info "Starting ETL pipeline for date range: $start_date to $end_date"
    print_info "Total days to process: $total_days"
    print_info "=========================================="
    
    # Process each date
    local current_date="$start_date"
    local processed_days=0
    local failed_days=0
    local start_time=$(date +%s)
    
    while is_date_before_or_equal "$current_date" "$end_date"; do
        ((processed_days++))
        print_info "Processing day $processed_days of $total_days: $current_date"
        
        if run_etl_for_date "$current_date"; then
            print_success "Day $processed_days/$total_days completed: $current_date"
        else
            print_error "Day $processed_days/$total_days failed: $current_date"
            ((failed_days++))
            
            # Ask user if they want to continue on failure
            echo ""
            print_warning "ETL failed for date: $current_date"
            read -p "Do you want to continue with the remaining dates? (y/n): " -n 1 -r
            echo ""
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                print_info "Stopping execution as requested by user"
                break
            fi
        fi
        
        # Move to next date
        current_date=$(get_next_date "$current_date")
        
        # Add a small delay between runs to avoid overwhelming the system
        if is_date_before_or_equal "$current_date" "$end_date"; then
            print_info "Waiting 10 seconds before next run..."
            sleep 10
        fi
    done
    
    # Calculate execution time
    local end_time=$(date +%s)
    local execution_time=$((end_time - start_time))
    local hours=$((execution_time / 3600))
    local minutes=$(((execution_time % 3600) / 60))
    local seconds=$((execution_time % 60))
    
    # Summary
    print_info "=========================================="
    print_info "ETL Pipeline Execution Summary:"
    print_info "Date range: $start_date to $end_date"
    print_info "Total days processed: $processed_days"
    print_info "Successful days: $((processed_days - failed_days))"
    print_info "Failed days: $failed_days"
    print_info "Execution time: ${hours}h ${minutes}m ${seconds}s"
    
    if [[ $failed_days -eq 0 ]]; then
        print_success "All ETL runs completed successfully!"
        exit 0
    else
        print_warning "ETL completed with $failed_days failures. Check individual log files for details."
        exit 1
    fi
}

# Run main function with all arguments
main "$@"
