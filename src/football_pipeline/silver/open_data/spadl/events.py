from socceraction.data.statsbomb import StatsBombLoader
import socceraction.spadl as spadl
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))

from football_pipeline.utils.constants import DATA_DIR

# Get paths dynamically when needed
source_path = "open_data/data"
landing_dir = DATA_DIR / "landing" / source_path
datafolder = str(landing_dir)  # Use the landing directory
SBL = StatsBombLoader(root=datafolder, getter="local")

# Load the data
events = SBL.events(7298)