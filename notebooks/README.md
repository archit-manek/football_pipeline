# Football Pipeline Notebooks

This directory contains Jupyter notebooks for exploring and testing the football data pipeline.

## 🚀 Quick Setup

### Option 1: Environment Variable (Recommended)
```bash
# Add to your ~/.bashrc or ~/.zshrc
export FOOTBALL_PIPELINE_ROOT="/Users/architmanek/Desktop/DataEngineering/football_pipeline"

# Reload your shell
source ~/.bashrc  # or source ~/.zshrc
```

### Option 2: Git-based (Automatic)
The notebooks will automatically detect the project root using Git. No setup required!

## 📚 Notebook Structure

1. **`01_explore_raw_data.ipynb`** - Raw data exploration
2. **`02_transform_silver_layer.ipynb`** - Silver layer transformations
3. **`03_debug_possessions.ipynb`** - Possession-specific debugging
4. **`04_gold_aggregation.ipynb`** - Gold layer aggregations
5. **`05_visualization.ipynb`** - Plots and reports

## 🔧 Usage Pattern

Every notebook should start with:
```python
# Standard notebook setup (4 lines)
import sys
from pathlib import Path
notebooks_dir = Path.cwd() if Path.cwd().name == 'notebooks' else Path.cwd() / 'notebooks'
if notebooks_dir.exists() and str(notebooks_dir) not in sys.path:
    sys.path.insert(0, str(notebooks_dir))

from notebook_utils import quick_setup
project_root, data_paths = quick_setup()

# Now you can import from the project
from silver.silver_transform import *
```

Or even simpler, just copy this template:
```python
# Copy-paste this template at the top of every notebook
import sys
from pathlib import Path
notebooks_dir = Path.cwd() if Path.cwd().name == 'notebooks' else Path.cwd() / 'notebooks'
if notebooks_dir.exists() and str(notebooks_dir) not in sys.path:
    sys.path.insert(0, str(notebooks_dir))
from notebook_utils import quick_setup
project_root, data_paths = quick_setup()
```

## 🛠 Available Utilities

### `notebook_utils.py`
- `quick_setup()` - One-line setup for project root and data paths
- `get_project_root()` - Robust project root detection
- `setup_notebook_environment()` - Configure Python paths
- `get_data_paths()` - Standardized data directory paths

### Data Paths Available
```python
data_paths = {
    'bronze_events': project_root / 'data' / 'bronze' / 'events',
    'bronze_matches': project_root / 'data' / 'bronze' / 'matches',
    'bronze_lineups': project_root / 'data' / 'bronze' / 'lineups',
    'silver_events': project_root / 'data' / 'silver' / 'events',
    'gold': project_root / 'data' / 'gold',
}
```

## 🐛 Troubleshooting

### Path Issues
If you see path errors, the notebook utilities will automatically:
1. Try environment variable `FOOTBALL_PIPELINE_ROOT`
2. Detect Git root directory
3. Look for marker files (`requirements.txt`, `setup.py`)
4. Assume notebooks are in `project_root/notebooks/`

### Import Errors
Make sure you're using the standard setup:
```python
from notebook_utils import quick_setup
project_root, data_paths = quick_setup()
```

This ensures the project root is added to Python's path.

## 🎯 Best Practices

1. **Always use `notebook_utils.quick_setup()`** at the start of notebooks
2. **Use `data_paths` dictionary** instead of hardcoded paths
3. **Test with real data** rather than fabricated test data
4. **Keep notebooks focused** - one main topic per notebook
5. **Use descriptive variable names** and add comments for complex logic

## 📁 File Organization

```
notebooks/
├── README.md                    # This file
├── notebook_utils.py           # Robust path utilities
├── 01_explore_raw_data.ipynb   # Raw data exploration
├── 02_transform_silver_layer.ipynb  # Silver transformations
├── 03_debug_possessions.ipynb  # Possession debugging
├── 04_gold_aggregation.ipynb   # Gold layer work
└── 05_visualization.ipynb      # Plots and reports
``` 