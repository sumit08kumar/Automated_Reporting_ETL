"""
Comprehensive test script for the Automated Reporting ETL Pipeline.
This script tests all components of the ETL system.
"""

import sys
import os
sys.path.append('src')

from src.report_generator import ReportGenerator
from pathlib import Path
import pandas as pd
import json

def test_basic_pipeline():
    """Test the basic ETL pipeline functionality."""
    print("=" * 60)
    print("TESTING BASIC ETL PIPELINE")
    print("=" * 60)
    
    # Initialize the report generator
    input_dir = "data/input"
    output_dir = "data/output"
    
    generator = ReportGenerator(input_dir, output_dir)
    
    # Run the complete pipeline
    print("\nRunning complete ETL pipeline...")
    results = generator.run_complete_pipeline()
    
    # Display results
    print(f"\nPipeline Status: {results['status']}")
    
    if results['status'] == 'success':
        print(f"Duration: {results['pipeline_duration_seconds']:.2f} seconds")
        print(f"Files processed: {results['data_summary']['input_files_processed']}")
        print(f"Rows processed: {results['data_summary']['total_rows_processed']}")
        print(f"Output files generated: {results['data_summary']['output_files_generated']}")
        
        print("\nGenerated output files:")
        for file_type, file_path in results['output_files'].items():
            print(f"  {file_type}: {file_path}")
            
        return True
    else:
        print(f"Pipeline failed with error: {results['error']}")
        return False

def test_individual_components():
    """Test individual ETL components."""
    print("\n" + "=" * 60)
    print("TESTING INDIVIDUAL COMPONENTS")
    print("=" * 60)
    
    from src.data_extractor import DataExtractor
    from src.data_transformer import DataTransformer
    from src.data_loader import DataLoader
    
    # Test Data Extractor
    print("\n1. Testing Data Extractor...")
    try:
        extractor = DataExtractor("data/input")
        
        # Get file info
        file_info = extractor.get_file_info()
        print(f"   Discovered {len(file_info)} files")
        
        # Extract data
        raw_data = extractor.extract_data()
        print(f"   Extracted data shape: {raw_data.shape}")
        print(f"   Columns: {list(raw_data.columns)}")
        
        print("   ✓ Data Extractor test passed")
        
    except Exception as e:
        print(f"   ✗ Data Extractor test failed: {str(e)}")
        return False
    
    # Test Data Transformer
    print("\n2. Testing Data Transformer...")
    try:
        transformer = DataTransformer()
        
        # Test transformation configuration
        config = {
            'standardize_columns': True,
            'handle_missing': True,
            'remove_duplicates': True,
            'convert_types': True,
            'type_mapping': {
                'date': 'datetime'
            },
            'calculations': {
                'profit_margin': 'total_amount * 0.2'
            }
        }
        
        transformed_data = transformer.transform_data(raw_data, config)
        print(f"   Transformed data shape: {transformed_data.shape}")
        
        # Get transformation summary
        summary = transformer.get_transformation_summary()
        print(f"   Applied {len(summary)} transformations")
        
        print("   ✓ Data Transformer test passed")
        
    except Exception as e:
        print(f"   ✗ Data Transformer test failed: {str(e)}")
        return False
    
    # Test Data Loader
    print("\n3. Testing Data Loader...")
    try:
        loader = DataLoader("data/output")
        
        # Test multiple format export
        output_files = loader.create_multi_format_report(
            transformed_data.head(100),  # Use smaller dataset for testing
            "test_report",
            formats=['csv', 'excel_styled', 'summary']
        )
        
        print(f"   Generated {len(output_files)} output files:")
        for format_name, file_path in output_files.items():
            print(f"     {format_name}: {file_path}")
        
        print("   ✓ Data Loader test passed")
        
    except Exception as e:
        print(f"   ✗ Data Loader test failed: {str(e)}")
        return False
    
    return True

def test_data_quality():
    """Test data quality handling."""
    print("\n" + "=" * 60)
    print("TESTING DATA QUALITY HANDLING")
    print("=" * 60)
    
    from src.data_extractor import DataExtractor
    from src.data_transformer import DataTransformer
    
    try:
        # Test with files that have data quality issues
        extractor = DataExtractor("data/input")
        transformer = DataTransformer()
        
        # Extract data including files with issues
        raw_data = extractor.extract_data()
        
        print(f"Raw data shape: {raw_data.shape}")
        print(f"Missing values: {raw_data.isnull().sum().sum()}")
        print(f"Duplicate rows: {raw_data.duplicated().sum()}")
        
        # Apply comprehensive transformation
        config = {
            'standardize_columns': True,
            'handle_missing': True,
            'remove_duplicates': True,
            'convert_types': True,
            'missing_strategy': {
                'unit_price': 'fill',
                'sales_rep': 'fill',
                'region': 'fill'
            }
        }
        
        clean_data = transformer.transform_data(raw_data, config)
        
        print(f"Clean data shape: {clean_data.shape}")
        print(f"Missing values after cleaning: {clean_data.isnull().sum().sum()}")
        print(f"Duplicate rows after cleaning: {clean_data.duplicated().sum()}")
        
        print("✓ Data quality test passed")
        return True
        
    except Exception as e:
        print(f"✗ Data quality test failed: {str(e)}")
        return False

def test_configuration_handling():
    """Test configuration file handling."""
    print("\n" + "=" * 60)
    print("TESTING CONFIGURATION HANDLING")
    print("=" * 60)
    
    try:
        # Create a test configuration file
        test_config = {
            'extraction': {
                'file_patterns': ['*.xlsx', '*.csv'],
                'add_source_column': True
            },
            'transformation': {
                'standardize_columns': True,
                'handle_missing': True,
                'remove_duplicates': True,
                'type_mapping': {
                    'date': 'datetime',
                    'total_amount': 'numeric'
                },
                'calculations': {
                    'revenue_per_unit': 'total_amount / quantity'
                },
                'kpi_config': {
                    'total_revenue': {
                        'type': 'simple',
                        'formula': 'total_amount.sum()'
                    }
                }
            },
            'loading': {
                'formats': ['csv', 'excel_styled', 'summary'],
                'create_visualizations': True,
                'summary_config': {
                    'custom_kpis': {
                        'Average Sale': 'total_amount.mean()',
                        'Total Revenue': 'total_amount.sum()'
                    }
                }
            }
        }
        
        # Save configuration
        config_file = "config/test_config.json"
        os.makedirs("config", exist_ok=True)
        
        with open(config_file, 'w') as f:
            json.dump(test_config, f, indent=2, default=str)
        
        # Test with configuration file
        generator = ReportGenerator("data/input", "data/output", config_file)
        
        # Run pipeline with custom configuration
        results = generator.run_complete_pipeline()
        
        if results['status'] == 'success':
            print("✓ Configuration handling test passed")
            return True
        else:
            print(f"✗ Configuration test failed: {results['error']}")
            return False
            
    except Exception as e:
        print(f"✗ Configuration handling test failed: {str(e)}")
        return False

def test_error_handling():
    """Test error handling and edge cases."""
    print("\n" + "=" * 60)
    print("TESTING ERROR HANDLING")
    print("=" * 60)
    
    from src.data_extractor import DataExtractor
    
    # Test with non-existent directory
    print("1. Testing non-existent input directory...")
    try:
        extractor = DataExtractor("non_existent_directory")
        print("   ✗ Should have raised FileNotFoundError")
        return False
    except FileNotFoundError:
        print("   ✓ Correctly handled non-existent directory")
    except Exception as e:
        print(f"   ✗ Unexpected error: {str(e)}")
        return False
    
    # Test with empty directory
    print("2. Testing empty input directory...")
    try:
        empty_dir = "data/empty_test"
        os.makedirs(empty_dir, exist_ok=True)
        
        extractor = DataExtractor(empty_dir)
        data = extractor.extract_data()
        
        if data.empty:
            print("   ✓ Correctly handled empty directory")
        else:
            print("   ✗ Should have returned empty DataFrame")
            return False
            
    except Exception as e:
        print(f"   ✗ Error handling empty directory: {str(e)}")
        return False
    
    print("✓ Error handling tests passed")
    return True

def generate_test_report(test_results):
    """Generate a summary test report."""
    print("\n" + "=" * 60)
    print("TEST SUMMARY REPORT")
    print("=" * 60)
    
    total_tests = len(test_results)
    passed_tests = sum(test_results.values())
    failed_tests = total_tests - passed_tests
    
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {failed_tests}")
    print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    print("\nTest Results:")
    for test_name, result in test_results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {test_name}: {status}")
    
    if failed_tests == 0:
        print("\n🎉 All tests passed! The ETL pipeline is working correctly.")
    else:
        print(f"\n⚠️  {failed_tests} test(s) failed. Please review the errors above.")
    
    return failed_tests == 0

def main():
    """Run all tests."""
    print("Starting comprehensive ETL pipeline testing...")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")
    
    # Dictionary to store test results
    test_results = {}
    
    # Run all tests
    test_results["Basic Pipeline"] = test_basic_pipeline()
    test_results["Individual Components"] = test_individual_components()
    test_results["Data Quality"] = test_data_quality()
    test_results["Configuration Handling"] = test_configuration_handling()
    test_results["Error Handling"] = test_error_handling()
    
    # Generate final report
    all_passed = generate_test_report(test_results)
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

