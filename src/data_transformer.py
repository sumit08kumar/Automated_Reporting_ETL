"""
Data Transformation Module for Automated Reporting ETL Pipeline

This module handles data cleaning, transformation, aggregation, and KPI calculation.
It provides comprehensive functionality to process raw data into analysis-ready format.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Union, Callable, Any
from datetime import datetime, timedelta
import re


class DataTransformer:
    """
    A class to handle data transformation, cleaning, and KPI calculation.
    """
    
    def __init__(self, log_level: str = "INFO"):
        """
        Initialize the DataTransformer.
        
        Args:
            log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        # Set up logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Store transformation history
        self.transformation_log = []
    
    def log_transformation(self, operation: str, details: str):
        """
        Log a transformation operation.
        
        Args:
            operation (str): Name of the operation
            details (str): Details about the operation
        """
        log_entry = {
            'timestamp': datetime.now(),
            'operation': operation,
            'details': details
        }
        self.transformation_log.append(log_entry)
        self.logger.info(f"{operation}: {details}")
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names by converting to lowercase and replacing spaces/special chars.
        
        Args:
            df (pd.DataFrame): Input DataFrame
        
        Returns:
            pd.DataFrame: DataFrame with standardized column names
        """
        original_columns = df.columns.tolist()
        
        # Standardize column names
        new_columns = []
        for col in df.columns:
            # Convert to lowercase
            new_col = str(col).lower()
            # Replace spaces and special characters with underscores
            new_col = re.sub(r'[^a-z0-9_]', '_', new_col)
            # Remove multiple consecutive underscores
            new_col = re.sub(r'_+', '_', new_col)
            # Remove leading/trailing underscores
            new_col = new_col.strip('_')
            new_columns.append(new_col)
        
        df.columns = new_columns
        
        self.log_transformation(
            "Column Standardization",
            f"Renamed {len(original_columns)} columns"
        )
        
        return df
    
    def handle_missing_values(self, 
                            df: pd.DataFrame, 
                            strategy: Dict[str, Union[str, Any]] = None) -> pd.DataFrame:
        """
        Handle missing values in the DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            strategy (Dict): Strategy for handling missing values per column
                           Format: {'column_name': 'drop'/'fill'/'forward'/'backward'/value}
        
        Returns:
            pd.DataFrame: DataFrame with missing values handled
        """
        if strategy is None:
            strategy = {}
        
        original_shape = df.shape
        missing_before = df.isnull().sum().sum()
        
        for column in df.columns:
            if column in strategy:
                method = strategy[column]
                
                if method == 'drop':
                    df = df.dropna(subset=[column])
                elif method == 'fill':
                    if df[column].dtype in ['int64', 'float64']:
                        df[column] = df[column].fillna(df[column].mean())
                    else:
                        df[column] = df[column].fillna('Unknown')
                elif method == 'forward':
                    df[column] = df[column].fillna(method='ffill')
                elif method == 'backward':
                    df[column] = df[column].fillna(method='bfill')
                else:
                    # Fill with specific value
                    df[column] = df[column].fillna(method)
            else:
                # Default strategy: fill numeric with mean, categorical with 'Unknown'
                if df[column].dtype in ['int64', 'float64']:
                    df[column] = df[column].fillna(df[column].mean())
                else:
                    df[column] = df[column].fillna('Unknown')
        
        missing_after = df.isnull().sum().sum()
        
        self.log_transformation(
            "Missing Value Handling",
            f"Reduced missing values from {missing_before} to {missing_after}. "
            f"Shape changed from {original_shape} to {df.shape}"
        )
        
        return df
    
    def remove_duplicates(self, 
                         df: pd.DataFrame, 
                         subset: Optional[List[str]] = None,
                         keep: str = 'first') -> pd.DataFrame:
        """
        Remove duplicate rows from the DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            subset (List[str], optional): Columns to consider for identifying duplicates
            keep (str): Which duplicates to keep ('first', 'last', False)
        
        Returns:
            pd.DataFrame: DataFrame with duplicates removed
        """
        original_shape = df.shape
        
        df = df.drop_duplicates(subset=subset, keep=keep)
        
        duplicates_removed = original_shape[0] - df.shape[0]
        
        self.log_transformation(
            "Duplicate Removal",
            f"Removed {duplicates_removed} duplicate rows. "
            f"Shape changed from {original_shape} to {df.shape}"
        )
        
        return df
    
    def convert_data_types(self, 
                          df: pd.DataFrame, 
                          type_mapping: Dict[str, str] = None) -> pd.DataFrame:
        """
        Convert data types of columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            type_mapping (Dict): Mapping of column names to desired data types
        
        Returns:
            pd.DataFrame: DataFrame with converted data types
        """
        if type_mapping is None:
            type_mapping = {}
        
        conversions_made = []
        
        for column, target_type in type_mapping.items():
            if column in df.columns:
                try:
                    original_type = df[column].dtype
                    
                    if target_type == 'datetime':
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    elif target_type == 'numeric':
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif target_type == 'category':
                        df[column] = df[column].astype('category')
                    else:
                        df[column] = df[column].astype(target_type)
                    
                    conversions_made.append(f"{column}: {original_type} -> {df[column].dtype}")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to convert {column} to {target_type}: {str(e)}")
        
        if conversions_made:
            self.log_transformation(
                "Data Type Conversion",
                f"Converted {len(conversions_made)} columns: {'; '.join(conversions_made)}"
            )
        
        return df
    
    def create_calculated_fields(self, 
                                df: pd.DataFrame, 
                                calculations: Dict[str, str] = None) -> pd.DataFrame:
        """
        Create calculated fields based on existing columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            calculations (Dict): Dictionary of new column names and their calculation formulas
        
        Returns:
            pd.DataFrame: DataFrame with new calculated fields
        """
        if calculations is None:
            calculations = {}
        
        fields_created = []
        
        for new_column, formula in calculations.items():
            try:
                # Use eval with DataFrame context (be careful with security in production)
                df[new_column] = df.eval(formula)
                fields_created.append(f"{new_column} = {formula}")
                
            except Exception as e:
                self.logger.warning(f"Failed to create calculated field {new_column}: {str(e)}")
        
        if fields_created:
            self.log_transformation(
                "Calculated Fields Creation",
                f"Created {len(fields_created)} fields: {'; '.join(fields_created)}"
            )
        
        return df
    
    def aggregate_data(self, 
                      df: pd.DataFrame, 
                      group_by: List[str], 
                      aggregations: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
        """
        Aggregate data by specified columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            group_by (List[str]): Columns to group by
            aggregations (Dict): Aggregation functions per column
        
        Returns:
            pd.DataFrame: Aggregated DataFrame
        """
        try:
            # Perform aggregation
            agg_df = df.groupby(group_by).agg(aggregations).reset_index()
            
            # Flatten column names if multi-level
            if isinstance(agg_df.columns, pd.MultiIndex):
                agg_df.columns = ['_'.join(col).strip('_') for col in agg_df.columns.values]
            
            self.log_transformation(
                "Data Aggregation",
                f"Aggregated data by {group_by}. "
                f"Shape changed from {df.shape} to {agg_df.shape}"
            )
            
            return agg_df
            
        except Exception as e:
            self.logger.error(f"Error in data aggregation: {str(e)}")
            raise
    
    def calculate_kpis(self, df: pd.DataFrame, kpi_config: Dict[str, Dict] = None) -> pd.DataFrame:
        """
        Calculate Key Performance Indicators (KPIs).
        
        Args:
            df (pd.DataFrame): Input DataFrame
            kpi_config (Dict): Configuration for KPI calculations
        
        Returns:
            pd.DataFrame: DataFrame with KPI columns added
        """
        if kpi_config is None:
            kpi_config = self._get_default_kpi_config()
        
        kpis_calculated = []
        
        for kpi_name, config in kpi_config.items():
            try:
                kpi_type = config.get('type', 'simple')
                
                if kpi_type == 'simple':
                    # Simple calculation based on formula
                    formula = config.get('formula', '')
                    if formula:
                        df[kpi_name] = df.eval(formula)
                        kpis_calculated.append(kpi_name)
                
                elif kpi_type == 'growth':
                    # Growth rate calculation
                    base_column = config.get('base_column', '')
                    period_column = config.get('period_column', '')
                    
                    if base_column and period_column:
                        df = df.sort_values(period_column)
                        df[kpi_name] = df[base_column].pct_change() * 100
                        kpis_calculated.append(kpi_name)
                
                elif kpi_type == 'ratio':
                    # Ratio calculation
                    numerator = config.get('numerator', '')
                    denominator = config.get('denominator', '')
                    
                    if numerator and denominator:
                        df[kpi_name] = df[numerator] / df[denominator]
                        kpis_calculated.append(kpi_name)
                
                elif kpi_type == 'cumulative':
                    # Cumulative sum
                    base_column = config.get('base_column', '')
                    if base_column:
                        df[kpi_name] = df[base_column].cumsum()
                        kpis_calculated.append(kpi_name)
                
            except Exception as e:
                self.logger.warning(f"Failed to calculate KPI {kpi_name}: {str(e)}")
        
        if kpis_calculated:
            self.log_transformation(
                "KPI Calculation",
                f"Calculated {len(kpis_calculated)} KPIs: {', '.join(kpis_calculated)}"
            )
        
        return df
    
    def _get_default_kpi_config(self) -> Dict[str, Dict]:
        """
        Get default KPI configuration.
        
        Returns:
            Dict: Default KPI configuration
        """
        return {
            'total_revenue': {
                'type': 'simple',
                'formula': 'quantity * price if "quantity" in @df.columns and "price" in @df.columns else 0'
            },
            'profit_margin': {
                'type': 'ratio',
                'numerator': 'profit',
                'denominator': 'revenue'
            }
        }
    
    def apply_business_rules(self, 
                           df: pd.DataFrame, 
                           rules: List[Dict] = None) -> pd.DataFrame:
        """
        Apply business rules to the data.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            rules (List[Dict]): List of business rules to apply
        
        Returns:
            pd.DataFrame: DataFrame with business rules applied
        """
        if rules is None:
            rules = []
        
        rules_applied = []
        
        for rule in rules:
            try:
                rule_name = rule.get('name', 'Unnamed Rule')
                condition = rule.get('condition', '')
                action = rule.get('action', '')
                
                if condition and action:
                    # Apply the rule
                    mask = df.eval(condition)
                    if action.startswith('set_'):
                        # Set column value
                        column, value = action.replace('set_', '').split('=')
                        df.loc[mask, column.strip()] = value.strip()
                    elif action.startswith('drop'):
                        # Drop rows
                        df = df[~mask]
                    
                    rules_applied.append(rule_name)
                    
            except Exception as e:
                self.logger.warning(f"Failed to apply business rule {rule.get('name', 'Unknown')}: {str(e)}")
        
        if rules_applied:
            self.log_transformation(
                "Business Rules Application",
                f"Applied {len(rules_applied)} rules: {', '.join(rules_applied)}"
            )
        
        return df
    
    def transform_data(self, 
                      df: pd.DataFrame, 
                      config: Dict = None) -> pd.DataFrame:
        """
        Apply a complete transformation pipeline to the data.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            config (Dict): Configuration for transformations
        
        Returns:
            pd.DataFrame: Fully transformed DataFrame
        """
        if config is None:
            config = {}
        
        self.logger.info("Starting data transformation pipeline")
        original_shape = df.shape
        
        # Step 1: Standardize column names
        if config.get('standardize_columns', True):
            df = self.standardize_column_names(df)
        
        # Step 2: Handle missing values
        if config.get('handle_missing', True):
            missing_strategy = config.get('missing_strategy', {})
            df = self.handle_missing_values(df, missing_strategy)
        
        # Step 3: Remove duplicates
        if config.get('remove_duplicates', True):
            duplicate_subset = config.get('duplicate_subset', None)
            df = self.remove_duplicates(df, duplicate_subset)
        
        # Step 4: Convert data types
        if config.get('convert_types', True):
            type_mapping = config.get('type_mapping', {})
            df = self.convert_data_types(df, type_mapping)
        
        # Step 5: Create calculated fields
        calculations = config.get('calculations', {})
        if calculations:
            df = self.create_calculated_fields(df, calculations)
        
        # Step 6: Apply business rules
        business_rules = config.get('business_rules', [])
        if business_rules:
            df = self.apply_business_rules(df, business_rules)
        
        # Step 7: Calculate KPIs
        kpi_config = config.get('kpi_config', {})
        if kpi_config:
            df = self.calculate_kpis(df, kpi_config)
        
        self.log_transformation(
            "Complete Transformation Pipeline",
            f"Transformation completed. Shape changed from {original_shape} to {df.shape}"
        )
        
        return df
    
    def get_transformation_summary(self) -> pd.DataFrame:
        """
        Get a summary of all transformations applied.
        
        Returns:
            pd.DataFrame: Summary of transformations
        """
        if not self.transformation_log:
            return pd.DataFrame()
        
        summary_df = pd.DataFrame(self.transformation_log)
        return summary_df


# Example usage and testing functions
def main():
    """
    Example usage of the DataTransformer class.
    """
    # Create sample data
    sample_data = pd.DataFrame({
        'Product Name': ['Product A', 'Product B', 'Product A', 'Product C', None],
        'Sales Amount': [1000, 1500, 1000, 800, 2000],
        'Quantity': [10, 15, 10, 8, 20],
        'Date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-03', '2024-01-04'],
        'Region': ['North', 'South', 'North', 'East', 'West']
    })
    
    print("Original data:")
    print(sample_data)
    print(f"Shape: {sample_data.shape}")
    
    # Initialize transformer
    transformer = DataTransformer()
    
    # Define transformation configuration
    config = {
        'standardize_columns': True,
        'handle_missing': True,
        'remove_duplicates': True,
        'convert_types': True,
        'type_mapping': {
            'date': 'datetime',
            'sales_amount': 'float64',
            'quantity': 'int64'
        },
        'calculations': {
            'unit_price': 'sales_amount / quantity',
            'revenue_category': 'sales_amount > 1200'
        },
        'kpi_config': {
            'total_revenue': {
                'type': 'simple',
                'formula': 'sales_amount'
            }
        }
    }
    
    # Apply transformations
    transformed_data = transformer.transform_data(sample_data, config)
    
    print("\nTransformed data:")
    print(transformed_data)
    print(f"Shape: {transformed_data.shape}")
    
    # Get transformation summary
    summary = transformer.get_transformation_summary()
    print("\nTransformation Summary:")
    print(summary[['operation', 'details']])


if __name__ == "__main__":
    main()

