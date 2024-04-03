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
    logger.info("Downloading kaggle dataset for Big Basket products")
    kaggle.api.dataset_download_files(dataset=dataset_path, path=raw_data_dir, unzip=True)



if __name__ == "__main__":
    dataset_path = config["BIG_BASKET"]["dataset_path"]
    raw_data_dir = config["COMMON"]["raw_data_dir"]
    download_kaggle_dataset(dataset_path,raw_data_dir)