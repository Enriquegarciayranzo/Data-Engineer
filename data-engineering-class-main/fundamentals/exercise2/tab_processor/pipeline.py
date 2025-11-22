import logging
import datetime
import subprocess
from pathlib import Path

LOGS_DIR = Path("./logs")
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOGS_DIR / "pipeline.log",
    filemode="w",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    encoding="utf-8"
)

def run_step(name, command):
    logging.info(f"Starting: {name}")
    print(f"Running: {name}")

    try:
        subprocess.check_call(command, shell=True)
        logging.info(f"{name} completed successfully")
        print(f"{name} OK")
    except Exception as e:
        logging.error(f"Error in {name}: {e}")
        print(f"Error in {name} --> logged")
        

def main():
    start = datetime.datetime.now()
    logging.info(f"PIPELINE STARTED at {start}")
    print("PIPELINE STARTED")

    run_step("CLEANER", "python tab_cleaner/main.py")
    run_step("VALIDATOR", "python tab_validator/main.py -i")
    run_step("RESULTS", "python results/main.py")
    run_step("LYRICS", "python lyrics/main.py")
    run_step("INSIGHTS", "python insights/main.py")
    run_step("STATS", "python stats/main.py")
    run_step("DUPLICATES", "python duplicates/main.py")

    end = datetime.datetime.now()
    duration = end - start
    
    logging.info(f"PIPELINE FINISHED at {end}")
    logging.info(f"TOTAL TIME: {duration}")

    print("\nPIPELINE FINISHED")
    print(f"Duration: {duration.total_seconds()} seconds")

if __name__ == "__main__":
    main()