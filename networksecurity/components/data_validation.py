from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

from scipy.stats import ks_2samp

import os
import sys
import pandas as pd

class DataValidation:
    def __init__(
            self, data_ingestion_artifact: DataIngestionArtifact,
            data_validation_config: DataValidationConfig
    ):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        """
        Reads a CSV file and returns a DataFrame.
        
        :param file_path: Path to the CSV file.
        :return: DataFrame containing the data from the CSV file.
        """
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def validate_number_of_columns(self, df: pd.DataFrame) -> bool:
        """
        Validates the number of columns in the DataFrame against the schema.
        
        :param df: DataFrame to validate.
        :return: True if the number of columns matches the schema, False otherwise.
        """
        try:
            number_of_columns = len(self._schema_config)
            logging.info(f'required Number of columns: {number_of_columns}')
            logging.info(f'Dataframe has columns: {len(df.columns)}')

            if len(df.columns) == number_of_columns:
                return True
            else:
                logging.error(f'Number of columns mismatch: expected {number_of_columns}, got {len(df.columns)}')
                return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def validate_numerical_columns_exist(self, df: pd.DataFrame) -> bool:
        """
        Validates that all numerical columns defined in the schema exist in the DataFrame.
        
        :param df: DataFrame to validate.
        :return: True if all numerical columns exist, False otherwise.
        """
        try:
            numerical_columns = self._schema_config.get('numerical_columns', [])
            missing_columns = [col for col in numerical_columns if col not in df.columns]
            if not missing_columns:
                return True
            else:
                logging.error(f'Missing numerical columns: {missing_columns}')
                return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def detect_dataset_drift(self, base_df, current_df, threshold=0.05) -> bool:
        """
        Detects dataset drift using the Kolmogorov-Smirnov test.
        
        :param base_df: Base DataFrame (e.g., training data).
        :param current_df: Current DataFrame (e.g., new data).
        :param threshold: Significance level for the KS test.
        :return: True if drift is detected, False otherwise.
        """
        try:
            status = True
            report = {}
            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]
                is_same_dist = ks_2samp(d1, d2)
                if threshold <= is_same_dist.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False
                report.update({column: {
                    'p_value': float(is_same_dist.pvalue),
                    'drift_status': is_found
                }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path

            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)

            write_yaml_file(file_path=drift_report_file_path, content=report)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def initiate_data_validation(self) ->DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            logging.info('Reading training and testing data files')
            train_df = DataValidation.read_data(train_file_path)
            test_df = DataValidation.read_data(test_file_path)

            logging.info('Validating number of columns in training and testing data')
            status = self.validate_number_of_columns(df=train_df) and self.validate_number_of_columns(df=test_df)
            if not status:
                error_message = f'Training and testing data does not contain all columns. \n'
                logging.info(error_message)
            
            logging.info('Validating numerical columns in training and testing data')
            status_numerical = self.validate_numerical_columns_exist(df=train_df) and self.validate_numerical_columns_exist(df=test_df)
            if not status_numerical:
                error_message = f'Training and testing data does not contain all numerical columns. \n'
                logging.info(error_message)
            
            logging.info('Detecting dataset drift between training and testing data')
            status_drift = self.detect_dataset_drift(
                base_df=train_df,
                current_df=test_df
            )
            dir_path= os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_df.to_csv(
                self.data_validation_config.valid_train_file_path, index=False, header=True
            )
            test_df.to_csv(
                self.data_validation_config.valid_test_file_path, index=False, header=True
            )

            data_validation_artifact = DataValidationArtifact(
                validation_status=status and status_numerical and status_drift,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
