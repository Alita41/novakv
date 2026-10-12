.PHONY: build test run benchmark clean
build:
	cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
	cmake --build build -j

test: build
	ctest --test-dir build --output-on-failure
	pytest -q tests/test_integration.py

run: build
	./build/novakv-server --port 7379 --workers 8 --wal data/novakv.wal

benchmark: build
	mkdir -p artifacts data
	./build/novakv-server --port 7379 --workers 8 --wal data/benchmark.wal & echo $$! > .server.pid
	sleep 0.5
	python benchmarks/benchmark.py --requests 20000 --clients 16 --out artifacts/benchmark.json
	kill `cat .server.pid` || true
	rm -f .server.pid data/benchmark.wal

clean:
	rm -rf build .pytest_cache __pycache__ data/*.wal artifacts/*.json
