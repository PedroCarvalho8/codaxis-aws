# Fontes -> template.yaml. O Git sync le so o template, entao ele e commitado
# como artefato: gerado, nunca editado a mao.

.PHONY: build check test clean

build:  ## regenera template.yaml a partir de infra/, lambdas/, glue/ e frontend/
	python3 build/assemble.py

check:  ## nao altera nada; e o que o CI roda
	python3 build/assemble.py --check
	cfn-lint template.yaml
	python3 -m compileall -q glue lambdas
	python3 tests/test_geohash.py
	python3 tests/test_argus_handlers.py
	python3 tools/check_spec_routes.py

test: check

clean:
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

spec-check:  ## rotas do gateway batem 1:1 com openapi/openapi.yaml
	python3 tools/check_spec_routes.py
