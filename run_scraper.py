import subprocess
import sys
import os


# Determine the path to the scraper module script
script_path = os.path.join(os.path.dirname(__file__), "scraper", "scraper.py")

# Run the scraper script using the current Python interpreter
subprocess.run([sys.executable, script_path])
