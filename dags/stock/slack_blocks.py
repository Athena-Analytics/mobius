"""There are some blocks of slack for notifications."""

dag_failure_slack_blocks = [
    {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "Dag Id: {{ dag.dag_id }}",
            "emoji": True,
        },
    },
    {
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": "*Tag:*\n{{ dag.tags }}"},
            {"type": "mrkdwn", "text": "*RunType:*\n{{ dag_run.run_type }}"},
            {"type": "mrkdwn", "text": "*RunId:*\n{{ dag_run.run_id }}"},
        ],
    },
]
