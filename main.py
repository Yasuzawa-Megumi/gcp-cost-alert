from google.cloud import bigquery
from datetime import datetime, timedelta, date
import json
import requests
import os
from datetime import datetime, timedelta, date

query = """
    SELECT
        invoice.month AS calq_month,
        currency,
        project.id AS project_id,
        Format("%.2f", (SUM(CAST(cost AS NUMERIC))
        + SUM(IFNULL((SELECT SUM(CAST(c.amount AS NUMERIC)) FROM UNNEST(credits) AS c), 0)))) AS cost
    FROM
        `gcp_dataset_name`
    WHERE CAST((RIGHT(invoice.month,2))AS INT) = EXTRACT(MONTH FROM CURRENT_DATE())
    GROUP BY project.id,invoice.month,currency
    ;
"""

#環境変数からSlackに通知するためのURLを取得
SLACK_WEBHOOK_URL = os.environ['SLACK_WEBHOOK_URL']


def post_slack(event, context) -> None:

    # GCP 利用料金取得
    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    info_text = '\n'.join(['{}：{} {}'.format(row.project_id, row.cost, row.currency) for row in rows])
    
    #コスト集計範囲の取得
    start_date, end_date = get_monthly_cost_date_range()

    # Slack通知内容の作成
    payload = {
        'attachments': [
            {
                'color': '#6495ed',
                'pretext': '今月のGCP利用費用の合計金額',
                'text': '期間：' + start_date
                    + '〜' + end_date
                    + ' \n'
                    + info_text
            }
        ]
    }

    # Slackへの通知
    try:
        response = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload))
    except requests.exceptions.RequestException as e:
        print(e)
    else:
        print(response.status_code)


# 対象月のコスト算出対象の初日と当日の日付を取得する
def get_monthly_cost_date_range():

    # Costを算出する期間を設定する
    start_date = date.today().replace(day=1).isoformat()
    end_date = date.today().isoformat()

    # 「start_date」と「end_date」が同じ場合「start_date」は先月の月初の値を取得する。
    if start_date == end_date:
        end_of_month = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=-1)
        begin_of_month = end_of_month.replace(day=1)
        return begin_of_month.date().isoformat(), end_date

    return start_date, end_date