import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def username():
	return os.getenv('USERNAME')

def password():
	return os.getenv('PASSWORD')

def host():
	return os.getenv('HOST')