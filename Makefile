IMAGE=scl3/task_hii_osm_csv


build:
	docker build --no-cache -t $(IMAGE) .

run:
	docker run --env-file=.env -v `pwd`/data:/data -v `pwd`/src:/app --rm -it --entrypoint python $(IMAGE) task.py --skip_cleanup

shell:
	docker run --env-file=.env -v `pwd`/tests:/tests -v `pwd`/data:/data -v `pwd`/src:/app --rm -it --entrypoint bash $(IMAGE)

cleanup:
	isort `pwd`/src/*.py
	black `pwd`/src/*.py
	flake8 `pwd`/src/*.py
	mypy `pwd`/src/*.py