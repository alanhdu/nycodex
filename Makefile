build-docker:
	docker build --tag nycodex-postgres .

stop-postgres:
	docker stop nycodex-postgres || true

postgres-up: build-docker stop-postgres
	docker run --rm \
	    --name nycodex-postgres \
	    --volume nycodex-postgres:/var/lib/postgresql/data \
	    --publish 5432:5432 \
	    -e POSTGRES_USER=adi \
	    -e POSTGRES_PASSWORD=password \
	    -e POSTGRES_DB=nycodex \
	    nycodex-postgres

clear-postgres: stop-postgres
	docker volume rm nycodex-postgres
