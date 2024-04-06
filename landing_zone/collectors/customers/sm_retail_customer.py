import kaggle
import logging
import configparser
import  os

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Get the path to the parent parent directory
config_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir, os.pardir))

# Specify the path to config file
config_file_path = os.path.join(config_dir, "config.ini")

config = configparser.ConfigParser()
config.read(config_file_path)

def download_kaggle_dataset(dataset_path,raw_data_dir):
    logger.info('-----------------------------------------------------')
    logger.info("Downloading customer data from kaggle dataset for Supermart Grocery Sales - Retail Analytics")
    kaggle.api.dataset_download_files(dataset=dataset_path, path=raw_data_dir, unzip=True)
    os.rename(os.path.join(raw_data_dir, 'Supermart Grocery Sales - Retail Analytics Dataset.csv'), os.path.join(raw_data_dir, 'sm_retail_customers.csv'))

if __name__ == "__main__":
    dataset_path = config["SM_RETAIL_CUSTOMERS"]["dataset_path"]
    raw_data_dir = config["COMMON"]["raw_data_dir"]
    download_kaggle_dataset(dataset_path,raw_data_dir)