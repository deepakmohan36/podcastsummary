from airflow.decorators import dag,task
import pendulum
import requests
import xmltodict
import os

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"

@dag(
    dag_id = 'podcast_summary',
    schedule_interval = '@daily',
    start_date = pendulum.datetime(2024,8,11),
    catchup = False
)

def podcast_summary():

    create_table = SqliteOperator(
    task_id='create_table',
    sql="""
    CREATE TABLE IF NOT EXISTS episodes (
        link TEXT PRIMARY KEY,
        title TEXT,
        filename TEXT,
        published TEXT,
        description TEXT,
        transcript TEXT
    );
    """,
    sqlite_conn_id='sqlite_default'
    )


    @task
    def get_episodes():
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes!")
        return episodes
    
    podcast_episodes = get_episodes()
    create_table.set_downstream(podcast_episodes)

    @task
    def load_data(episodes):
        hook = SqliteHook(sqlite_conn_id='sqlite_default')
        stored = hook.get_pandas_df('SELECT * FROM episodes;')
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])
        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=["link", "title", "published", "description", "filename"])
        return new_episodes

    new_episodes = load_data(podcast_episodes)

    @task()
    def download_episodes(episodes):
        audio_files = []
        for episode in episodes[:3]:
            name_end = episode["link"].split('/')[-1]
            filename = f"{name_end}.mp3"
            audio_path = os.path.join("episodes", filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)
            audio_files.append({
                "link": episode["link"],
                "filename": filename
            })
        return audio_files

    audio_files = download_episodes(podcast_episodes)

podcast_summary()