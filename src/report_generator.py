"""
Report Generator Module for Automated Reporting ETL Pipeline

This module orchestrates the complete ETL pipeline and provides
high-level interfaces for report generation and automation.
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Union, Any
from pathlib import Path
from datetime import datetime
import json
import yaml

from data_extractor import DataExtractor
from data_transformer import DataTransformer
from data_loader import DataLoader


class ReportGenerator:
    """
    Main class that orchestrates the complete ETL pipeline for automated reporting.
    """
    
    def __init__(self, 
                 input_directory: str,
                 output_directory: str,
                 config_file: Optional[str] = None,
                 log_level: str = "INFO"):
        """
        Initialize the ReportGenerator.
        
        Args:
            input_directory (str): Path to input data directory
            output_directory (str): Path to output directory
            config_file (str, optional): Path to configuration file
            log_level (str): Logging level
        """
        self.input_directory = Path(input_directory)
        self.output_directory = Path(output_directory)
        self.config_file = config_file
        
        # Create output directory if it doesn't exist
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        # Set up logging
        log_file = self.output_directory / f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize ETL components
        self.extractor = DataExtractor(str(self.input_directory), log_level)
        self.transformer = DataTransformer(log_level)
        self.loader = DataLoader(str(self.output_directory), log_level)
        
        # Load configuration
        self.config = self._load_config()
        
        # Store pipeline results
        self.pipeline_results = {}
        
        self.logger.info("ReportGenerator initialized successfully")
    
    def _load_config(self) -> Dict:
        """
        Load configuration from file or return default configuration.
        
        Returns:
            Dict: Configuration dictionary
        """
        if self.config_file and Path(self.config_file).exists():
            try:
                with open(self.config_file, 'r') as f:
                    if self.config_file.endswith('.yaml') or self.config_file.endswith('.yml'):
                        config = yaml.safe_load(f)
                    else:
                        config = json.load(f)
                
                self.logger.info(f"Loaded configuration from {self.config_file}")
                return config
                
            except Exception as e:
                self.logger.warning(f"Failed to load config file {self.config_file}: {str(e)}")
        
        # Return default configuration
        return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """
        Get default configuration for the ETL pipeline.
        
        Returns:
            Dict: Default configuration
        """
        return {
            'extraction': {
                'file_patterns': ['*.xlsx', '*.xls', '*.csv'],
                'add_source_column': True
            },
            'transformation': {
                'standardize_columns': True,
                'handle_missing': True,
                'remove_duplicates': True,
                'convert_types': True,
                'type_mapping': {},
                'calculations': {},
                'business_rules': [],
                'kpi_config': {}
            },
            'loading': {
                'formats': ['csv', 'excel_styled', 'summary'],
                'create_visualizations': True,
                'summary_config': {
                    'custom_kpis': {}
                }
            },
            'reporting': {
                'base_filename': f"automated_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'include_metadata': True,
                'create_dashboard': False
            }
        }
    
    def run_extraction(self, **kwargs) -> pd.DataFrame:
        """
        Run the data extraction phase.
        
        Args:
            **kwargs: Additional arguments for extraction
        
        Returns:
            pd.DataFrame: Extracted data
        """
        self.logger.info("Starting data extraction phase")
        
        extraction_config = self.config.get('extraction', {})
        extraction_config.update(kwargs)
        
        # Extract data
        raw_data = self.extractor.extract_data(
            file_patterns=extraction_config.get('file_patterns'),
            add_source_column=extraction_config.get('add_source_column', True)
        )
        
        # Store results
        self.pipeline_results['extraction'] = {
            'data_shape': raw_data.shape,
            'columns': list(raw_data.columns),
            'extraction_time': datetime.now()
        }
        
        self.logger.info(f"Data extraction completed. Shape: {raw_data.shape}")
        return raw_data
    
    def run_transformation(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Run the data transformation phase.
        
        Args:
            data (pd.DataFrame): Raw data to transform
            **kwargs: Additional arguments for transformation
        
        Returns:
            pd.DataFrame: Transformed data
        """
        self.logger.info("Starting data transformation phase")
        
        transformation_config = self.config.get('transformation', {})
        transformation_config.update(kwargs)
        
        # Apply transformations
        transformed_data = self.transformer.transform_data(data, transformation_config)
        
        # Store results
        self.pipeline_results['transformation'] = {
            'original_shape': data.shape,
            'transformed_shape': transformed_data.shape,
            'transformation_log': self.transformer.get_transformation_summary(),
            'transformation_time': datetime.now()
        }
        
        self.logger.info(f"Data transformation completed. Shape: {transformed_data.shape}")
        return transformed_data
    
    def run_loading(self, data: pd.DataFrame, **kwargs) -> Dict[str, str]:
        """
        Run the data loading phase.
        
        Args:
            data (pd.DataFrame): Transformed data to load
            **kwargs: Additional arguments for loading
        
        Returns:
            Dict[str, str]: Dictionary of generated file paths
        """
        self.logger.info("Starting data loading phase")
        
        loading_config = self.config.get('loading', {})
        loading_config.update(kwargs)
        
        reporting_config = self.config.get('reporting', {})
        base_filename = reporting_config.get('base_filename', 'automated_report')
        
        # Create reports in multiple formats
        output_files = self.loader.create_multi_format_report(
            data,
            base_filename,
            formats=loading_config.get('formats', ['csv', 'excel_styled', 'summary']),
            config=loading_config.get('summary_config', {})
        )
        
        # Create visualizations if requested
        if loading_config.get('create_visualizations', True):
            visualizations = self._create_visualizations(data, base_filename)
            output_files.update(visualizations)
        
        # Store results
        self.pipeline_results['loading'] = {
            'output_files': output_files,
            'loading_time': datetime.now()
        }
        
        self.logger.info(f"Data loading completed. Generated {len(output_files)} files")
        return output_files
    
    def _create_visualizations(self, data: pd.DataFrame, base_filename: str) -> Dict[str, str]:
        """
        Create visualizations for the data.
        
        Args:
            data (pd.DataFrame): Data to visualize
            base_filename (str): Base filename for visualizations
        
        Returns:
            Dict[str, str]: Dictionary of visualization file paths
        """
        visualizations = {}
        
        try:
            # Get numeric and categorical columns
            numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
            categorical_cols = data.select_dtypes(include=['object', 'category']).columns.tolist()
            
            # Remove metadata columns
            categorical_cols = [col for col in categorical_cols 
                              if col not in ['source_file', 'source_path']]
            
            # Create bar chart if we have categorical and numeric data
            if len(categorical_cols) > 0 and len(numeric_cols) > 0:
                # Aggregate data for visualization
                cat_col = categorical_cols[0]
                num_col = numeric_cols[0]
                
                # Limit to top 10 categories to avoid cluttered charts
                top_categories = data[cat_col].value_counts().head(10).index
                viz_data = data[data[cat_col].isin(top_categories)]
                agg_data = viz_data.groupby(cat_col)[num_col].sum().reset_index()
                
                viz_path = self.loader.create_visualization(
                    agg_data,
                    'bar',
                    f"{base_filename}_bar_chart",
                    x=cat_col,
                    y=num_col,
                    title=f'{num_col} by {cat_col}'
                )
                visualizations['bar_chart'] = viz_path
            
            # Create correlation heatmap if we have multiple numeric columns
            if len(numeric_cols) > 1:
                viz_path = self.loader.create_visualization(
                    data[numeric_cols],
                    'heatmap',
                    f"{base_filename}_correlation_heatmap",
                    title='Correlation Matrix'
                )
                visualizations['heatmap'] = viz_path
            
            # Create time series if we have date columns
            date_cols = data.select_dtypes(include=['datetime64']).columns.tolist()
            if len(date_cols) > 0 and len(numeric_cols) > 0:
                date_col = date_cols[0]
                num_col = numeric_cols[0]
                
                # Aggregate by date
                time_data = data.groupby(data[date_col].dt.date)[num_col].sum().reset_index()
                time_data.columns = [date_col, num_col]
                
                viz_path = self.loader.create_visualization(
                    time_data,
                    'line',
                    f"{base_filename}_time_series",
                    x=date_col,
                    y=num_col,
                    title=f'{num_col} over Time'
                )
                visualizations['time_series'] = viz_path
                
        except Exception as e:
            self.logger.warning(f"Failed to create some visualizations: {str(e)}")
        
        return visualizations
    
    def run_complete_pipeline(self, **kwargs) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline from extraction to loading.
        
        Args:
            **kwargs: Additional arguments for any phase
        
        Returns:
            Dict[str, Any]: Complete pipeline results
        """
        self.logger.info("Starting complete ETL pipeline")
        pipeline_start_time = datetime.now()
        
        try:
            # Phase 1: Extraction
            raw_data = self.run_extraction(**kwargs.get('extraction', {}))
            
            if raw_data.empty:
                raise ValueError("No data was extracted. Please check input files.")
            
            # Phase 2: Transformation
            transformed_data = self.run_transformation(
                raw_data, 
                **kwargs.get('transformation', {})
            )
            
            # Phase 3: Loading
            output_files = self.run_loading(
                transformed_data, 
                **kwargs.get('loading', {})
            )
            
            # Create metadata report
            if self.config.get('reporting', {}).get('include_metadata', True):
                metadata_file = self._create_metadata_report()
                output_files['metadata'] = metadata_file
            
            # Calculate total pipeline time
            pipeline_end_time = datetime.now()
            pipeline_duration = (pipeline_end_time - pipeline_start_time).total_seconds()
            
            # Compile final results
            final_results = {
                'status': 'success',
                'pipeline_duration_seconds': pipeline_duration,
                'data_summary': {
                    'input_files_processed': len(self.extractor.discover_files()),
                    'total_rows_processed': len(transformed_data),
                    'total_columns': len(transformed_data.columns),
                    'output_files_generated': len(output_files)
                },
                'output_files': output_files,
                'pipeline_results': self.pipeline_results
            }
            
            self.logger.info(f"ETL pipeline completed successfully in {pipeline_duration:.2f} seconds")
            return final_results
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'pipeline_results': self.pipeline_results
            }
    
    def _create_metadata_report(self) -> str:
        """
        Create a metadata report with pipeline information.
        
        Returns:
            str: Path to the metadata report file
        """
        try:
            metadata = {
                'pipeline_execution': {
                    'execution_time': datetime.now().isoformat(),
                    'input_directory': str(self.input_directory),
                    'output_directory': str(self.output_directory),
                    'config_file': self.config_file
                },
                'pipeline_results': self.pipeline_results,
                'configuration': self.config
            }
            
            # Convert to DataFrame for easy viewing
            metadata_items = []
            
            def flatten_dict(d, parent_key='', sep='_'):
                items = []
                for k, v in d.items():
                    new_key = f"{parent_key}{sep}{k}" if parent_key else k
                    if isinstance(v, dict):
                        items.extend(flatten_dict(v, new_key, sep=sep).items())
                    else:
                        items.append((new_key, str(v)))
                return dict(items)
            
            flat_metadata = flatten_dict(metadata)
            metadata_df = pd.DataFrame(list(flat_metadata.items()), 
                                     columns=['Metadata_Item', 'Value'])
            
            # Save as Excel file
            reporting_config = self.config.get('reporting', {})
            base_filename = reporting_config.get('base_filename', 'automated_report')
            metadata_file = self.loader.save_to_excel_styled(
                metadata_df, 
                f"{base_filename}_metadata.xlsx",
                sheet_name='Pipeline_Metadata'
            )
            
            self.logger.info(f"Created metadata report: {metadata_file}")
            return metadata_file
            
        except Exception as e:
            self.logger.error(f"Failed to create metadata report: {str(e)}")
            return ""
    
    def save_config(self, config_path: str):
        """
        Save current configuration to a file.
        
        Args:
            config_path (str): Path to save the configuration file
        """
        try:
            config_path = Path(config_path)
            
            if config_path.suffix.lower() in ['.yaml', '.yml']:
                with open(config_path, 'w') as f:
                    yaml.dump(self.config, f, default_flow_style=False)
            else:
                with open(config_path, 'w') as f:
                    json.dump(self.config, f, indent=2, default=str)
            
            self.logger.info(f"Configuration saved to {config_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save configuration: {str(e)}")
    
    def get_pipeline_summary(self) -> pd.DataFrame:
        """
        Get a summary of the pipeline execution.
        
        Returns:
            pd.DataFrame: Pipeline summary
        """
        if not self.pipeline_results:
            return pd.DataFrame()
        
        summary_data = []
        
        for phase, results in self.pipeline_results.items():
            if isinstance(results, dict):
                for key, value in results.items():
                    if key.endswith('_time'):
                        continue  # Skip time entries for now
                    summary_data.append({
                        'Phase': phase.title(),
                        'Metric': key.replace('_', ' ').title(),
                        'Value': str(value)
                    })
        
        return pd.DataFrame(summary_data)


# Example usage and testing functions
def main():
    """
    Example usage of the ReportGenerator class.
    """
    # Configuration
    input_dir = "../data/input"
    output_dir = "../data/output"
    
    try:
        # Initialize report generator
        generator = ReportGenerator(input_dir, output_dir)
        
        # Run complete pipeline
        results = generator.run_complete_pipeline()
        
        print("Pipeline Results:")
        print(f"Status: {results['status']}")
        
        if results['status'] == 'success':
            print(f"Duration: {results['pipeline_duration_seconds']:.2f} seconds")
            print(f"Files processed: {results['data_summary']['input_files_processed']}")
            print(f"Rows processed: {results['data_summary']['total_rows_processed']}")
            print(f"Output files: {results['data_summary']['output_files_generated']}")
            
            print("\nGenerated files:")
            for file_type, file_path in results['output_files'].items():
                print(f"  {file_type}: {file_path}")
        else:
            print(f"Error: {results['error']}")
        
        # Get pipeline summary
        summary = generator.get_pipeline_summary()
        if not summary.empty:
            print("\nPipeline Summary:")
            print(summary.to_string(index=False))
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")


if __name__ == "__main__":
    main()

