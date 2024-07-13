"""Dag is a demo designed to test some notifier."""
import pendulum
from airflow.decorators import dag, task

from airflow.providers.discord.notifications.discord import DiscordNotifier
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier


dag_success_discord_webhook_notification = DiscordNotifier(
    discord_conn_id ="discord_webhook_airflow",
    text="The dag {{ dag.dag_id }} success"
)


task_success_discord_webhook_notification = DiscordNotifier(
    discord_conn_id ="discord_webhook_airflow",
    text="The dag {{ ti.task_id }} success"
)


dag_failure_slack_webhook_notification = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_webhook_mobius",
    text="The dag {{ dag.dag_id }} failed",
    blocks=[
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
                    "text": "*Owner:*\n{{ dag.owner }}"
                }
            ]
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": "*When:*\n{{ ds }}"
                }
            ]
        },
    ]
)

task_failure_slack_webhook_notification = SlackWebhookNotifier(
    slack_webhook_conn_id="slack_webhook_mobius",
    text="The task {{ ti.task_id }} failed"
)


@dag(
    "demo_notifier",
    start_date=pendulum.today(),
    schedule=None,
    catchup=False,
    tags=["demo"],
    on_success_callback=[dag_success_discord_webhook_notification],
    on_failure_callback=[dag_failure_slack_webhook_notification],
)
def demo_notifier():
    """
    Test some notifiers that is
    - discord notifier
    - slack notifier
    > [Discord Webhook Tutorial](https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks)

    > [Slack API Documentation](https://api.slack.com/docs)
    
    > [Slack Messaging Documentation](https://api.slack.com/messaging/interactivity)
    """
    @task.bash(
        env={"MY_VAR": "Hello World"},
        on_success_callback=[task_success_discord_webhook_notification],
        on_failure_callback=[task_failure_slack_webhook_notification]
    )
    def success_task():
        return "echo $MY_VAR"

    @task.bash(
        on_success_callback=[task_success_discord_webhook_notification],
        on_failure_callback=[task_failure_slack_webhook_notification]
    )
    def fail_task():
        return "fail"

    success_task()
    fail_task()


dag_object = demo_notifier()


if __name__ == "__main__":
    dag_object.test()
