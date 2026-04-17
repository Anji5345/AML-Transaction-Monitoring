pipeline {
    agent {
        node {
            customWorkspace '/workspace/aml-monitoring-pipeline'
        }
    }

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        PYTHONUNBUFFERED = '1'
        PIP_DISABLE_PIP_VERSION_CHECK = '1'
    }

    stages {
        stage('Python Environment') {
            steps {
                sh '''
                    python3 --version
                    python3 -m venv .venv-jenkins
                    . .venv-jenkins/bin/activate
                    python -m pip install --upgrade pip
                    python -m pip install -r requirements.txt
                '''
            }
        }

        stage('Syntax Check') {
            steps {
                sh '''
                    . .venv-jenkins/bin/activate
                    python -m compileall producer flink notification dashboard airflow/dags tests
                '''
            }
        }

        stage('Dataset Sanity Check') {
            steps {
                sh '''
                    . .venv-jenkins/bin/activate
                    python - <<'PY'
import pandas as pd

path = "data/raw/aml_project_test_paysim.csv"
df = pd.read_csv(path)

required_columns = {
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
}

missing_columns = required_columns - set(df.columns)
if missing_columns:
    raise AssertionError(f"Missing columns: {sorted(missing_columns)}")

if len(df) != 100:
    raise AssertionError(f"Expected 100 rows, found {len(df)}")

print("Dataset sanity check passed")
PY
                '''
            }
        }

        stage('Unit Tests') {
            steps {
                sh '''
                    . .venv-jenkins/bin/activate
                    pytest -q
                '''
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: 'logs/*.log', allowEmptyArchive: true
        }
    }
}
