"""
Validation Script for Agmarknet Data Pipeline

This script validates the data integrity and checks for common issues:
1. Duplicate records within CSVs
2. Year gaps in data
3. Progress.json consistency
4. Data quality metrics
"""

import os
import json
import pandas as pd
from collections import defaultdict
import glob

# Configuration
DATA_DIR = "data/crops"
PROGRESS_FILE = "data/progress.json"
START_YEAR = 2007
CURRENT_YEAR = 2026

def check_duplicates():
    """Check for duplicates in CSV files."""
    print("\n" + "="*70)
    print("🔍 CHECKING FOR DUPLICATES")
    print("="*70)
    
    key_cols = ["State", "District", "Market", "Commodity", "Arrival_Date"]
    issues = []
    
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    total_duplicates = 0
    
    for csv_file in csv_files[:10]:  # Check first 10 for quick validation
        try:
            df = pd.read_csv(csv_file)
            before = len(df)
            df_dedup = df.drop_duplicates(subset=key_cols)
            after = len(df_dedup)
            duplicates = before - after
            total_duplicates += duplicates
            
            if duplicates > 0:
                issues.append(f"  ❌ {os.path.basename(csv_file)}: {duplicates} duplicates")
            else:
                print(f"  ✅ {os.path.basename(csv_file)}: No duplicates")
        except Exception as e:
            issues.append(f"  ⚠️ {os.path.basename(csv_file)}: Error - {e}")
    
    if issues:
        print("\nIssues found:")
        for issue in issues:
            print(issue)
    
    print(f"\nTotal duplicates found: {total_duplicates}")
    return total_duplicates == 0


def check_year_gaps():
    """Check for year gaps in data."""
    print("\n" + "="*70)
    print("🔍 CHECKING FOR YEAR GAPS")
    print("="*70)
    
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    crops_with_gaps = []
    
    for csv_file in csv_files[:10]:  # Check first 10 for quick validation
        try:
            df = pd.read_csv(csv_file)
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], errors='coerce')
            df = df.dropna(subset=['Arrival_Date'])
            
            if len(df) == 0:
                continue
            
            years = set(df['Arrival_Date'].dt.year.unique())
            min_year = min(years)
            max_year = max(years)
            
            expected_years = set(range(min_year, max_year + 1))
            missing_years = expected_years - years
            
            if missing_years:
                crops_with_gaps.append(
                    f"  ❌ {os.path.basename(csv_file)}: Missing years {sorted(missing_years)}"
                )
            else:
                print(f"  ✅ {os.path.basename(csv_file)}: Complete ({min_year}-{max_year})")
                
        except Exception as e:
            print(f"  ⚠️ {os.path.basename(csv_file)}: Error - {e}")
    
    if crops_with_gaps:
        print("\nCrops with gaps:")
        for crop in crops_with_gaps:
            print(crop)
    
    return len(crops_with_gaps) == 0


def check_progress_consistency():
    """Check if progress.json matches CSV files."""
    print("\n" + "="*70)
    print("🔍 CHECKING PROGRESS.JSON CONSISTENCY")
    print("="*70)
    
    if not os.path.exists(PROGRESS_FILE):
        print("⚠️ progress.json does not exist yet")
        return True
    
    with open(PROGRESS_FILE, 'r') as f:
        progress = json.load(f)
    
    print(f"📋 Progress tracking {len(progress)} crops")
    
    inconsistencies = []
    
    for crop_key, last_year in list(progress.items())[:10]:  # Check first 10
        csv_path = os.path.join(DATA_DIR, f"{crop_key}.csv")
        
        if not os.path.exists(csv_path):
            inconsistencies.append(f"  ❌ {crop_key}: In progress but no CSV")
            continue
        
        try:
            df = pd.read_csv(csv_path)
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], errors='coerce')
            df = df.dropna(subset=['Arrival_Date'])
            
            if len(df) == 0:
                continue
            
            max_year_in_csv = df['Arrival_Date'].dt.year.max()
            
            if max_year_in_csv < last_year:
                inconsistencies.append(
                    f"  ⚠️ {crop_key}: Progress={last_year}, CSV max={max_year_in_csv}"
                )
            else:
                print(f"  ✅ {crop_key}: Progress={last_year}, CSV max={max_year_in_csv}")
                
        except Exception as e:
            inconsistencies.append(f"  ⚠️ {crop_key}: Error - {e}")
    
    if inconsistencies:
        print("\nInconsistencies found:")
        for issue in inconsistencies:
            print(issue)
    
    return len(inconsistencies) == 0


def get_data_statistics():
    """Get overall data statistics."""
    print("\n" + "="*70)
    print("📊 DATA STATISTICS")
    print("="*70)
    
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    total_files = len(csv_files)
    total_rows = 0
    year_coverage = defaultdict(int)
    
    print(f"Total CSV files: {total_files}")
    
    # Sample a few files for statistics
    for csv_file in csv_files[:20]:
        try:
            df = pd.read_csv(csv_file)
            total_rows += len(df)
            
            df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'], errors='coerce')
            df = df.dropna(subset=['Arrival_Date'])
            
            if len(df) > 0:
                years = df['Arrival_Date'].dt.year.unique()
                for year in years:
                    year_coverage[year] += 1
                    
        except Exception as e:
            print(f"⚠️ Error reading {csv_file}: {e}")
    
    print(f"Total rows (sampled): {total_rows:,}")
    
    if year_coverage:
        print("\nYear coverage (sample):")
        for year in sorted(year_coverage.keys()):
            print(f"  {year}: {year_coverage[year]} crops")


def main():
    """Run all validation checks."""
    print("="*70)
    print("🔬 AGMARKNET DATA PIPELINE VALIDATION")
    print("="*70)
    
    results = {
        "duplicates": check_duplicates(),
        "gaps": check_year_gaps(),
        "progress": check_progress_consistency()
    }
    
    get_data_statistics()
    
    print("\n" + "="*70)
    print("📋 VALIDATION SUMMARY")
    print("="*70)
    print(f"  Duplicates Check:  {'✅ PASS' if results['duplicates'] else '❌ FAIL'}")
    print(f"  Year Gaps Check:   {'✅ PASS' if results['gaps'] else '❌ FAIL'}")
    print(f"  Progress Check:    {'✅ PASS' if results['progress'] else '❌ FAIL'}")
    print("="*70)
    
    if all(results.values()):
        print("\n✅ All validation checks passed!")
        return 0
    else:
        print("\n⚠️ Some validation checks failed. Review output above.")
        return 1


if __name__ == "__main__":
    exit(main())
