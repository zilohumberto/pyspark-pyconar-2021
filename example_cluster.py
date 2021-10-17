
example_cluster_config = {
    "new_cluster": {
        "num_workers": 4,
        "spark_version": "7.5.x-scala2.12",
        "spark_conf": {},
        "azure_attributes": {
            "first_on_demand": 1,
            "availability": "ON_DEMAND_AZURE",
            "spot_bid_max_price": -1
        },
        "node_type_id": "Standard_DS3_v2",  # 14 GB Memory, 4 cores
        "driver_node_type_id": "Standard_DS3_v2",
        "ssh_public_keys": [],
        "custom_tags": {},
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": True,
        "cluster_source": "UI",
        "init_scripts": []
    },
    "libraries": [
    ],
    "spark_python_task": {"python_file": "dbfs:/FileStore/spotify_analyzer.py", "parameters": []}
}
