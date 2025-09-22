#!/usr/bin/env python3
"""
Script to add source mapping to consolidated DAX queries.
Analyzes DAX queries and identifies which source tables are being used.
Adds a 'Sources' column with comma-separated source table names.
"""

import pandas as pd
import openpyxl
import re
import sys

def get_source_tables():
    """
    Get the list of source tables from sources.txt file.
    """
    source_tables = [
        'Ordernhm',
        'Cancellation code', 
        'dimdate',
        'Social',
        'cred',
        'Itemnhm',
        'Seller NP'
    ]
    return source_tables

def extract_source_tables_from_dax(dax_query):
    """
    Extract source table names from DAX query.
    Returns a list of unique source tables found.
    """
    if pd.isna(dax_query) or dax_query == '':
        return []
    
    source_tables = get_source_tables()
    found_tables = []
    dax_str = str(dax_query)
    
    # Look for table references in various DAX patterns
    for table in source_tables:
        # Pattern 1: Table[column] - most common pattern
        if re.search(rf'\b{re.escape(table)}\[', dax_str, re.IGNORECASE):
            found_tables.append(table)
        # Pattern 2: 'Table' - quoted table names
        elif re.search(rf"'{re.escape(table)}'", dax_str, re.IGNORECASE):
            found_tables.append(table)
        # Pattern 3: Table.column - dot notation
        elif re.search(rf'\b{re.escape(table)}\.', dax_str, re.IGNORECASE):
            found_tables.append(table)
        # Pattern 4: Just the table name as a word boundary
        elif re.search(rf'\b{re.escape(table)}\b', dax_str, re.IGNORECASE):
            found_tables.append(table)
    
    # Remove duplicates while preserving order
    unique_tables = []
    for table in found_tables:
        if table not in unique_tables:
            unique_tables.append(table)
    
    return unique_tables

def add_source_mapping(input_file, output_file):
    """
    Add source mapping to the consolidated Excel file.
    """
    print(f"Processing file: {input_file}")
    
    # Read the consolidated Excel file
    df = pd.read_excel(input_file)
    
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    
    # Add Sources column
    df['Sources'] = ''
    
    print("Analyzing DAX queries for source tables...")
    
    # Process each row to extract sources from DAX queries
    for index, row in df.iterrows():
        dax_query = row['Measures']
        sources = extract_source_tables_from_dax(dax_query)
        
        # Join sources with comma and space
        sources_str = ', '.join(sources) if sources else ''
        df.at[index, 'Sources'] = sources_str
        
        if index < 10:  # Show first 10 for debugging
            print(f"Row {index} ({row['Measures Name']}): Found sources: {sources_str}")
    
    # Reorder columns to put Sources before Measures
    columns = ['Table_name', 'Measures Name', 'Sources', 'Measures']
    df = df[columns]
    
    # Save the updated file
    df.to_excel(output_file, index=False)
    print(f"Updated file saved as: {output_file}")
    
    # Show summary statistics
    source_counts = {}
    for sources_str in df['Sources']:
        if sources_str:
            tables = [s.strip() for s in sources_str.split(',')]
            for table in tables:
                source_counts[table] = source_counts.get(table, 0) + 1
    
    print("\nSource table usage summary:")
    for table, count in sorted(source_counts.items()):
        print(f"  {table}: {count} measures")
    
    # Show examples of complex DAX queries with multiple sources
    print("\nExamples of DAX queries with multiple sources:")
    multi_source_count = 0
    for index, row in df.iterrows():
        if row['Sources'] and ',' in row['Sources']:
            multi_source_count += 1
            print(f"  {row['Measures Name']}: {row['Sources']}")
            if multi_source_count >= 5:  # Show first 5 examples
                break
    
    return df

def main():
    input_file = "/home/dataorc/workspace_ondc/nodata-pipline/drax/Dax query - Retail_Logistics_consolidated.xlsx"
    output_file = "/home/dataorc/workspace_ondc/nodata-pipline/drax/Dax query - Retail_Logistics_with_sources.xlsx"
    
    try:
        print("=== ADDING SOURCE MAPPING ===")
        result_df = add_source_mapping(input_file, output_file)
        
        print(f"\n✅ Success! File with source mapping created: {output_file}")
        print(f"Total measures processed: {len(result_df)}")
        
        # Show final structure
        print(f"\nFinal columns: {list(result_df.columns)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

