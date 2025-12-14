import pytest
from package_analysis.models import AnalysisTask
from package_analysis.tasks import run_dynamic_analysis

@pytest.fixture
def analysis_task():
    return AnalysisTask.objects.create(
        package_name="test_package",
        package_version="1.0.0",
        ecosystem="test_ecosystem"
    )

def test_run_dynamic_analysis(analysis_task):
    result = run_dynamic_analysis.delay(analysis_task.id)
    assert result.status == "SUCCESS"