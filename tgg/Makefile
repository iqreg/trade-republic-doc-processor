.PHONY: native-setup native-run native-test worker demo docker-up docker-down docker-logs

native-setup:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt
	@if [ ! -f .env ]; then cp .env.native.example .env; fi

native-run:
	. .venv/bin/activate && PYTHONPATH=.. uvicorn tgg.apps.api.main:app --host 127.0.0.1 --port 8000 --reload

native-test:
	. .venv/bin/activate && PYTHONPATH=.. pytest -q tests

worker:
	. .venv/bin/activate && PYTHONPATH=.. python -m tgg.apps.worker.pipeline

demo:
	. .venv/bin/activate && PYTHONPATH=.. python infra/scripts/demo_walkthrough.py

# Optional Docker workflows (not required for local Mac testing)
docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f --tail=100
