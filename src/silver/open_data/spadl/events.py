from socceraction.data.statsbomb import StatsBombLoader
import socceraction.spadl as spadl
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))

from src.utils.constants import get_open_data_dirs

OPEN_DATA_DIRS = get_open_data_dirs()
LANDING_DIR = OPEN_DATA_DIRS["landing"]
datafolder = str(OPEN_DATA_DIRS["landing"])  # Use the landing directory
SBL = StatsBombLoader(root=datafolder, getter="local")

# Load the data
events = SBL.events(7298)