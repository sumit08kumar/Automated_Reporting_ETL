"""
Data Extraction Module for Automated Reporting ETL Pipeline

This module handles the extraction of data from Excel and CSV files.
It provides functionality to read multiple files from specified directories
and combine them into a unified dataset.
"""

import os
import glob
import pandas as pd
import logging
from typing import List, Dict, Optional, Union
from pathlib import Path


class DataExtractor:
    """
    A class to handle data extraction from Excel and CSV files.
    """
    
    def __init__(self, input_directory: str, log_level: str = "INFO"):
        """
        Initialize the DataExtractor.
        
        Args:
            input_directory (str): Path to the directory containing input files
            log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.input_directory = Path(input_directory)
        self.supported_formats = ['.xlsx', '.xls', '.csv']
        
        # Set up logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Validate input directory
        if not self.input_directory.exists():
            raise FileNotFoundError(f"Input directory does not exist: {input_directory}")
    
    def discover_files(self, file_patterns: Optional[List[str]] = None) -> List[Path]:
        """
        Discover all supported files in the input directory.
        
        Args:
            file_patterns (List[str], optional): Specific file patterns to search for.
                                               If None, searches for all supported formats.
        
        Returns:
            List[Path]: List of discovered file paths
        """
        discovered_files = []
        
        if file_patterns is None:
            # Default patterns for all supported formats
            file_patterns = ['*.xlsx', '*.xls', '*.csv']
        
        for pattern in file_patterns:
            files = list(self.input_directory.glob(pattern))
            discovered_files.extend(files)
            self.logger.info(f"Found {len(files)} files matching pattern '{pattern}'")
        
        # Remove duplicates and sort
        discovered_files = sorted(list(set(discovered_files)))
        self.logger.info(f"Total discovered files: {len(discovered_files)}")
        
        return discovered_files
    
    def read_excel_file(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """
        Read an Excel file and return a DataFrame.
        
        Args:
            file_path (Path): Path to the Excel file
            **kwargs: Additional arguments to pass to pd.read_excel()
        
        Returns:
            pd.DataFrame: Data from the Excel file
        """
        try:
            # Default parameters for Excel reading
            excel_params = {
                'sheet_name': 0,  # Read first sheet by default
                'header': 0,      # First row as header
                'engine': 'openpyxl'
            }
            excel_params.update(kwargs)
            
            df = pd.read_excel(file_path, **excel_params)
            self.logger.debug(f"Successfully read Excel file: {file_path}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading Excel file {file_path}: {str(e)}")
            raise
    
    def read_csv_file(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """
        Read a CSV file and return a DataFrame.
        
        Args:
            file_path (Path): Path to the CSV file
            **kwargs: Additional arguments to pass to pd.read_csv()
        
        Returns:
            pd.DataFrame: Data from the CSV file
        """
        try:
            # Default parameters for CSV reading
            csv_params = {
                'encoding': 'utf-8',
                'sep': ',',
                'header': 0
            }
            csv_params.update(kwargs)
            
            # Try different encodings if utf-8 fails
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings_to_try:
                try:
                    csv_params['encoding'] = encoding
                    df = pd.read_csv(file_path, **csv_params)
                    self.logger.debug(f"Successfully read CSV file: {file_path} with encoding: {encoding}")
                    return df
                except UnicodeDecodeError:
                    continue
            
            # If all encodings fail, raise the last exception
            raise UnicodeDecodeError(f"Could not decode file {file_path} with any of the attempted encodings")
            
        except Exception as e:
            self.logger.error(f"Error reading CSV file {file_path}: {str(e)}")
            raise
    
    def read_single_file(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """
        Read a single file (Excel or CSV) and return a DataFrame.
        
        Args:
            file_path (Path): Path to the file
            **kwargs: Additional arguments to pass to the appropriate reader
        
        Returns:
            pd.DataFrame: Data from the file
        """
        file_extension = file_path.suffix.lower()
        
        if file_extension in ['.xlsx', '.xls']:
            return self.read_excel_file(file_path, **kwargs)
        elif file_extension == '.csv':
            return self.read_csv_file(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    
    def extract_data(self, 
                    file_patterns: Optional[List[str]] = None,
                    add_source_column: bool = True,
                    **read_kwargs) -> pd.DataFrame:
        """
        Extract data from all discovered files and combine into a single DataFrame.
        
        Args:
            file_patterns (List[str], optional): Specific file patterns to search for
            add_source_column (bool): Whether to add a column indicating the source file
            **read_kwargs: Additional arguments to pass to file readers
        
        Returns:
            pd.DataFrame: Combined data from all files
        """
        files = self.discover_files(file_patterns)
        
        if not files:
            self.logger.warning("No files found to extract data from")
            return pd.DataFrame()
        
        all_dataframes = []
        
        for file_path in files:
            try:
                self.logger.info(f"Processing file: {file_path.name}")
                df = self.read_single_file(file_path, **read_kwargs)
                
                # Add source file information if requested
                if add_source_column:
                    df['source_file'] = file_path.name
                    df['source_path'] = str(file_path)
                
                all_dataframes.append(df)
                self.logger.info(f"Successfully processed {file_path.name}: {len(df)} rows")
                
            except Exception as e:
                self.logger.error(f"Failed to process file {file_path.name}: {str(e)}")
                continue
        
        if not all_dataframes:
            self.logger.error("No files were successfully processed")
            return pd.DataFrame()
        
        # Combine all DataFrames
        try:
            combined_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
            self.logger.info(f"Successfully combined {len(all_dataframes)} files into {len(combined_df)} total rows")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Error combining DataFrames: {str(e)}")
            raise
    
    def get_file_info(self, file_patterns: Optional[List[str]] = None) -> Dict[str, Dict]:
        """
        Get information about discovered files without reading their content.
        
        Args:
            file_patterns (List[str], optional): Specific file patterns to search for
        
        Returns:
            Dict[str, Dict]: Dictionary with file information
        """
        files = self.discover_files(file_patterns)
        file_info = {}
        
        for file_path in files:
            try:
                stat = file_path.stat()
                file_info[str(file_path)] = {
                    'name': file_path.name,
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'modified_time': stat.st_mtime,
                    'extension': file_path.suffix.lower()
                }
            except Exception as e:
                self.logger.error(f"Error getting info for file {file_path}: {str(e)}")
                file_info[str(file_path)] = {'error': str(e)}
        
        return file_info


# Example usage and testing functions
def main():
    """
    Example usage of the DataExtractor class.
    """
    # Example configuration
    input_dir = "../data/input"
    
    try:
        # Initialize extractor
        extractor = DataExtractor(input_dir)
        
        # Get file information
        file_info = extractor.get_file_info()
        print("Discovered files:")
        for file_path, info in file_info.items():
            print(f"  {info.get('name', 'Unknown')}: {info.get('size_mb', 0)} MB")
        
        # Extract data
        combined_data = extractor.extract_data()
        print(f"\nExtracted data shape: {combined_data.shape}")
        
        if not combined_data.empty:
            print(f"Columns: {list(combined_data.columns)}")
            print(f"Sample data:\n{combined_data.head()}")
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")


if __name__ == "__main__":
    main()

