"""Alert Slack when batch-assign finds resources with an empty transformed output.

current_resource_empty means the pipeline produced zero rows for a resource - usually a
broken source feed or pipeline config, not something a data reviewer can action from the
summary CSV alone. That CSV only gets opened when someone goes looking, so this posts a
prompt alert instead of leaving it to be noticed later.
"""
import glob
import os
import sys

import pandas as pd
import requests

SLACK_API_URL = "https://slack.com/api/chat.postMessage"
SLACK_CHANNEL = "planning-data-alerts"
MAX_LISTED = 20


def find_empty_resources():
    affected = []
    for path in glob.glob("batch_assign_summary*.csv"):
        df = pd.read_csv(path, dtype=str)
        if "error_code" not in df.columns:
            continue
        rows = df[df["error_code"] == "current_resource_empty"]
        affected.extend(rows[["dataset", "resource"]].to_dict("records"))
    return affected


def build_payload(scope, affected):
    lines = [f"• `{row['dataset']}` / `{row['resource']}`" for row in affected[:MAX_LISTED]]
    if len(affected) > MAX_LISTED:
        lines.append(f"...and {len(affected) - MAX_LISTED} more")

    run_url = (
        f"https://github.com/{os.environ.get('GITHUB_REPOSITORY', '')}"
        f"/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"
    )

    return {
        "channel": SLACK_CHANNEL,
        "text": f":warning: Batch assign ({scope}) found resources with no entities to assign",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": ":warning: current_resource_empty flagged"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"Batch assign (*{scope}*) found {len(affected)} resource(s) with no "
                        f"entities in the transformed output - usually an upstream source or "
                        f"pipeline problem, worth an engineer checking:\n" + "\n".join(lines)
                    ),
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"<{run_url}|View workflow run>"},
            },
        ],
    }


def post_to_slack(payload):
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("SLACK_BOT_TOKEN not set, skipping Slack alert")
        return

    response = requests.post(
        SLACK_API_URL,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    if not response.json().get("ok"):
        print(f"Slack API returned an error: {response.json()}")


def main():
    scope = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    affected = find_empty_resources()
    if not affected:
        print("No current_resource_empty resources found this run")
        return

    print(f"Found {len(affected)} current_resource_empty resource(s), alerting Slack")
    post_to_slack(build_payload(scope, affected))


if __name__ == "__main__":
    main()
