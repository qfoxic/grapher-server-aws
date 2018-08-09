venv:
	rm -rf ~/grapher-venv; python3 -m venv ~/grapher-venv && . ~/grapher-venv/bin/activate && pip3 install grapher-aws

develop:
	rm -rf ~/grapher-venv; python3 -m venv ~/grapher-venv && . ~/grapher-venv/bin/activate && pip3 install -e .
run:
	. ~/grapher-venv/bin/activate && python3 -m grapher.core.server -p 9999

deploy:
	python3 setup.py sdist upload -r pypi	
