docker exec -it bigdata-demo-airflow-webserver-1 bash -c "\
  python -c '\
import sys, traceback; \
from pathlib import Path; \
sys.path.append(\"/opt/app\"); \
from build_silver_gold import build_silver; \
print(\"Running build_silver manually...\"); \
build_silver();
'"
