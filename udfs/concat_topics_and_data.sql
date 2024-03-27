create or replace function concat_topics_and_data(topics array, data varchar)
returns varchar
language python runtime_version = '3.8'
handler = 'concat_topics_and_data'
as $$
def concat_topics_and_data(topics: list, data:str) -> str:
    """
    Converts array of topics and string of data to string of decodeable event data
    """
    try:
        log_data = "".join(topic[2:] for topic in topics[1:])
        log_data += data[2:]
        return log_data
    except:
        return None
$$
;
