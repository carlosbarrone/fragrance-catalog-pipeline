from pathlib import Path

import pytest


airflow = pytest.importorskip("airflow")
from airflow.models import DagBag


def test_dag_imports_cleanly():
	dag_folder = Path(__file__).resolve().parents[2] / "dags"
	dag_bag = DagBag(dag_folder=str(dag_folder), include_examples=False)

	assert dag_bag.import_errors == {}
	assert "ulta_products_sync" in dag_bag.dag_ids
