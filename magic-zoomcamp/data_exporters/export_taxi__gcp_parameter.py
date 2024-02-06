if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(df, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """

    now = kwargs.get('execution_date')
    now_fpath = now.strftime("%Y/%m/%d")

    print(now)

    print(now.strftime("%Y/%m/%d"))

    # Specify your data exporting logic here
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'de-course-411610-demo-bucket'
    object_key = f'{now_fpath}/daily-trips.parquet'
    print(object_key)

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )


