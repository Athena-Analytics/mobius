"""There are some blocks of slack for notifications."""
dag_failure_slack_blocks = [
    {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "*Dag Id:*\n{{ dag.dag_id }}",
            "emoji": True
        }
    },
    {
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": "*Tag:*\n{{ dag.tags }}"
            },
            {
                "type": "mrkdwn",
                "text": "*When:*\n{{ ds }}"
            }
        ]
    }
]


task_failure_slack_blocks = [
    {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "*Task Id:*\n{{ ti.task_id }}",
            "emoji": True
        }
    },
    {
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": "*Tag:*\n{{ dag.tags }}"
            },
            {
                "type": "mrkdwn",
                "text": "*When:*\n{{ ds }}"
            }
        ]
    }
]
