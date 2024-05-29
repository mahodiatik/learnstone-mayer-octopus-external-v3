import re
import json
import logging
from glob import glob
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TARGET_COURSES_JSON = "courses_leeds_2024-2025_2024-05-27T10:03:11.json"


if __name__ == "__main__":
    university_alias = re.findall(r"courses_(\w+)_.*", TARGET_COURSES_JSON).pop()

    with open(f"../data/courses/output/{university_alias}/{TARGET_COURSES_JSON}", 'r') as f:
        courses = json.load(f)

    academic_year = TARGET_COURSES_JSON.split("_")[-2]
    timestamp = re.findall(r"\d+-\d+-\d+T\d+:\d+:\d+", TARGET_COURSES_JSON).pop()

    data_missing, data_multiple = [], []
    for course in courses:
        if not course["qualification"]:
            data_missing.append({
                "university_alias": university_alias,
                "academic_year": academic_year,
                "timestamp": timestamp,
                "url": course["link"]
            })
        elif "," in course["qualification"]:
            data_multiple.append({
                "university_alias": university_alias,
                "academic_year": academic_year,
                "timestamp": timestamp,
                "url": course["link"],
                "qualification": course["qualification"]
            })

    df_missing = pd.DataFrame(data_missing)
    df_multiple = pd.DataFrame(data_multiple)

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    Path(f"data/overall/{today}").mkdir(parents=True, exist_ok=True)

    if data_missing:
        df_missing_path = f"data/overall/{today}/missing_qualification_stats_{today}.csv"
        df_missing = df_missing.sort_values(by=["academic_year", "university_alias"], ascending=True)
        df_missing = df_missing.drop_duplicates()
        df_missing.to_csv(df_missing_path, index=False)

        logger.info("Missing qualification stats saved to path %s" % df_missing_path)
    else:
        logger.info("All courses have qualifications set!")

    if data_multiple:
        df_multiple_path = f"data/overall/{today}/multiple_qualification_stats_{today}.csv"
        df_multiple = df_multiple.sort_values(by=["academic_year", "university_alias"], ascending=True) \
            if len(df_multiple) > 0 else df_multiple
        df_multiple = df_multiple.drop_duplicates()
        df_multiple.to_csv(df_multiple_path, index=False)

        logger.info("Multiple qualification stats saved to path %s" % df_multiple_path)
    else:
        logger.info("All courses have a single qualification!")
