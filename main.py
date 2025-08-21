#!/usr/bin/env python3
"""
Main entry point for the Automated Reporting ETL Pipeline.
This script provides a simple command-line interface to run the ETL pipeline.
"""

import sys
import argparse
from pathlib import Path
import json

# Add src directory to path
sys.path.append('src')

from src.report_generator import ReportGenerator

def main():
    """Main function to run the ETL pipeline."""
    parser = argparse.ArgumentParser(
        description='Automated Reporting ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                                    # Run with default settings
  python main.py -i data/input -o data/output      # Specify directories
  python main.py -c config.json                    # Use configuration file
  python main.py --generate-sample-data            # Generate sample data first
        """
    )
    
    parser.add_argument(
        '-i', '--input-dir',
        default='data/input',
        help='Input directory containing Excel/CSV files (default: data/input)'
    )
    
    parser.add_argument(
        '-o', '--output-dir', 
        default='data/output',
        help='Output directory for generated reports (default: data/output)'
    )
    
    parser.add_argument(
        '-c', '--config',
        help='Configuration file (JSON or YAML)'
    )
    
    parser.add_argument(
        '--generate-sample-data',
        action='store_true',
        help='Generate sample data files for testing'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--formats',
        nargs='+',
        choices=['csv', 'excel', 'excel_styled', 'summary'],
        default=['csv', 'excel_styled', 'summary'],
        help='Output formats to generate'
    )
    
    args = parser.parse_args()
    
    # Generate sample data if requested
    if args.generate_sample_data:
        print("Generating sample data...")
        try:
            import create_sample_data
            create_sample_data.main()
            print("Sample data generated successfully!")
        except Exception as e:
            print(f"Error generating sample data: {e}")
            return 1
    
    # Check if input directory exists and has files
    input_path = Path(args.input_dir)
    if not input_path.exists():
        print(f"Error: Input directory '{args.input_dir}' does not exist.")
        print("Use --generate-sample-data to create sample files, or specify a different directory with -i")
        return 1
    
    # Count input files
    file_patterns = ['*.xlsx', '*.xls', '*.csv']
    input_files = []
    for pattern in file_patterns:
        input_files.extend(list(input_path.glob(pattern)))
    
    if not input_files:
        print(f"Warning: No Excel or CSV files found in '{args.input_dir}'")
        print("Use --generate-sample-data to create sample files")
        return 1
    
    print(f"Found {len(input_files)} input files to process")
    
    # Initialize and run the ETL pipeline
    try:
        print("Initializing ETL pipeline...")
        generator = ReportGenerator(
            input_directory=args.input_dir,
            output_directory=args.output_dir,
            config_file=args.config,
            log_level=args.log_level
        )
        
        # Configure loading options
        loading_config = {
            'formats': args.formats
        }
        
        print("Starting ETL pipeline execution...")
        results = generator.run_complete_pipeline(loading=loading_config)
        
        # Display results
        print("\n" + "="*60)
        print("ETL PIPELINE RESULTS")
        print("="*60)
        
        if results['status'] == 'success':
            print("✅ Pipeline completed successfully!")
            print(f"⏱️  Duration: {results['pipeline_duration_seconds']:.2f} seconds")
            print(f"📁 Files processed: {results['data_summary']['input_files_processed']}")
            print(f"📊 Rows processed: {results['data_summary']['total_rows_processed']:,}")
            print(f"📄 Output files generated: {results['data_summary']['output_files_generated']}")
            
            print("\n📋 Generated Files:")
            for file_type, file_path in results['output_files'].items():
                file_size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
                file_size_mb = file_size / (1024 * 1024)
                print(f"  {file_type:20} {file_path} ({file_size_mb:.1f} MB)")
            
            print(f"\n📂 All files saved to: {args.output_dir}")
            print("\n🎉 ETL pipeline completed successfully!")
            return 0
            
        else:
            print("❌ Pipeline failed!")
            print(f"Error: {results['error']}")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

