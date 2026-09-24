"""DAG integrity test (runs only where Airflow is installed, e.g. the CI 'dag' job)."""
import os

import pytest

pytest.importorskip("airflow")


@pytest.mark.parametrize("mode", ["local", "aws"])
def test_dag_loads_with_expected_shape(mode, monkeypatch):
    monkeypatch.setenv("SIGNALS_RUN_MODE", mode)
    from airflow.models import DagBag

    bag = DagBag(dag_folder=os.path.join(os.path.dirname(__file__), "..", "dags"), include_examples=False)
    assert bag.import_errors == {}
    dag = bag.get_dag("support_signals_lakehouse")
    assert dag.max_active_runs == 1
    assert [t.task_id for t in dag.topological_sort()][:5] == [
        "ingest_github", "bronze_to_silver", "dq_silver", "silver_to_gold", "dq_gold"]
    assert set(dag.get_task("dq_gold").downstream_task_ids) == {"ml_topics_and_risk", "load_warehouse"}
    assert all(t.retries == 3 for t in dag.tasks)
