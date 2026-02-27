## Local Run

Install dependencies:

pip install -r requirements.txt

python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

Metrics are printed to the terminal and saved to `metrics.json`. Logs are written to `run.log`.


## Docker Build & Run

Build the image:

docker build -t mlops-task .

Run the container:

docker run --rm mlops-task


## Example Output — metrics.json

  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.4991,
  "latency_ms": 18,
  "seed": 42,
  "status": "success"