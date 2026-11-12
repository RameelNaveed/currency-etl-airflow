"""Basic checks: the DAG imports cleanly and has the expected 3 tasks in order."""
from airflow.models import DagBag


def test_currency_dag_loads():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert not dagbag.import_errors, dagbag.import_errors
    dag = dagbag.get_dag("currency_exchange_etl")
    assert dag is not None
    assert [t.task_id for t in dag.tasks] == [
        "extract_exchange_rates",
        "transform_exchange_rates",
        "load_to_mongodb",
    ]
    assert dag.get_task("load_to_mongodb").upstream_task_ids == {"transform_exchange_rates"}
