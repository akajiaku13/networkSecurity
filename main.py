import sys
from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.entity.config_entity import DataIngestionConfig, DataValidationConfig, DataTransformationConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

if __name__ == "__main__":
    try:
        trainingpipelineconfig = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)
        data_ingestion = DataIngestion(dataingestionconfig)
        logging.info("Exporting collection as DataFrame")

        dataingestionartifact = data_ingestion.initiate_data_ingestion()
        print(dataingestionartifact)
        print(' '*50)
        logging.info('Data Ingestion completed successfully')

        data_validation_config = DataValidationConfig(trainingpipelineconfig)
        data_validation = DataValidation(dataingestionartifact, data_validation_config)
        logging.info("Validating data")
        data_validation_artifact = data_validation.initiate_data_validation()
        logging.info("Data Validation completed successfully")
        print(data_validation_artifact)
        print(' '*50)

        data_transformation_config = DataTransformationConfig(trainingpipelineconfig)
        data_transformation = DataTransformation(data_validation_artifact, data_transformation_config)
        data_transformation_artifact = data_transformation.initiate_data_transformation()
        print(data_transformation_artifact)
        print(' '*50)
        logging.info('Data Transformation completed')
        
    except Exception as e:
        raise NetworkSecurityException(e, sys)