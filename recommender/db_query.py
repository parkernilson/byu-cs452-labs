## This script is used to query the database
import psycopg2

from db import CONNECTION

def fmt_result(list_of_tuples: list[tuple]):
    return '\n\n'.join(map(lambda t: '\n'.join([str(v) for v in t]), list_of_tuples))

def get_5_similar_segments(id: str):
    with psycopg2.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT p.title, s2.id, s2.content, s2.start_time, s2.end_time, s1.embedding <-> s2.embedding as l2_distance
                FROM podcast_segment s1, podcast_segment s2
                JOIN podcast p ON p.id = s2.podcast_id
                WHERE s1.id = '{id}' and s2.id != '{id}'
                ORDER BY s1.embedding <-> s2.embedding
                LIMIT 5
            """)
            return cur.fetchall()

def get_5_disimilar_segments(id: str):
    with psycopg2.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT p.title, s2.id, s2.content, s2.start_time, s2.end_time, s1.embedding <-> s2.embedding as l2_distance
                FROM podcast_segment s1, podcast_segment s2
                JOIN podcast p ON p.id = s2.podcast_id
                WHERE s1.id = '{id}' and s2.id != '{id}'
                ORDER BY s1.embedding <-> s2.embedding DESC
                LIMIT 5
            """)
            return cur.fetchall()

def get_5_similar_episodes_to_segment(segment_id: str):
    with psycopg2.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT p.title, s1.embedding <-> AVG(s2.embedding) as l2_distance
                FROM podcast_segment s1, podcast_segment s2
                JOIN podcast p ON p.id = s2.podcast_id
                WHERE s1.id = '{segment_id}' and s2.podcast_id != s1.podcast_id
                GROUP BY s2.podcast_id, p.title, s1.embedding
                ORDER BY s1.embedding <-> AVG(s2.embedding)
                LIMIT 5
            """)
            return cur.fetchall()

def get_5_similar_episodes_to_episode(episode_id: str):
    with psycopg2.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
WITH pod_to_compare AS (
    SELECT s.podcast_id, AVG(s.embedding) as podcast_embedding
    FROM podcast p
    JOIN podcast_segment s ON p.id = s.podcast_id
    WHERE p.id = '{episode_id}'
    GROUP BY s.podcast_id
)
SELECT p.title, pod.podcast_embedding <-> AVG(s2.embedding) as l2_distance
FROM pod_to_compare pod, podcast_segment s2
JOIN podcast p ON p.id = s2.podcast_id
WHERE s2.podcast_id != pod.podcast_id
GROUP BY s2.podcast_id, p.title, pod.podcast_embedding
ORDER BY pod.podcast_embedding <-> AVG(s2.embedding)
LIMIT 5
            """)
            return cur.fetchall()

# print(fmt_result(get_5_similar_segments('267:476')))
# print(fmt_result(get_5_disimilar_segments('267:476')))
# print(fmt_result(get_5_similar_segments('48:511')))
# print(fmt_result(get_5_similar_segments('51:56')))
# print(fmt_result(get_5_similar_episodes_to_segment('267:476')))
# print(fmt_result(get_5_similar_episodes_to_segment('48:511')))
# print(fmt_result(get_5_similar_episodes_to_segment('51:56')))
print(fmt_result(get_5_similar_episodes_to_episode('VeH7qKZr0WI')))
