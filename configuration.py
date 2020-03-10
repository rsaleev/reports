import os
from configparser import RawConfigParser
from datetime import datetime
from pathlib import Path

CONFIG_FILE = (str(Path(str(Path(__file__).parents[2]) + "/configuration/config.ini")))
parser = RawConfigParser()
parser.read(CONFIG_FILE)


wp_cnx = {"user": parser.get("WISEPARK", "rdbs_user"),
          "password": parser.get("WISEPARK", "rdbs_password"),
          "host": parser.get("WISEPARK", "rdbs_host"),
          "db": parser.get("WISEPARK", "rdbs_db"),
          "port": parser.getint("WISEPARK", "rdbs_port")}

parking_id = parser.getint("AMPP", "parking_id")
parking_addr = parser.get("AMPP", "parking_address")

if not os.path.isdir(str(Path(str(Path(__file__).parents[0]) + "/temp"))):
    os.mkdir(str(Path(str(Path(__file__).parents[0]) + "/temp")))
temp = (str(Path(str(Path(__file__).parents[0]) + "/temp")))
resources = (str(Path(str(Path(__file__).parents[2]) + "/resources/ampp")))
TEMPLATES = (str(Path(str(Path(__file__).parents[0]) + "/templates")))


asgi_host = parser.get("REPORT", "asgi_host")
asgi_port = parser.getint("REPORT", "asgi_port")
asgi_workers = parser.getint("REPORT", "asgi_workers")
asgi_debug = parser.getboolean("REPORT", "asgi_debug")
