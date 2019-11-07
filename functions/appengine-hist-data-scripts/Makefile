prepare-dev:
	mkdir -p secrets
	cp -R config/dev/* secrets/

deploy-dev: prepare-dev
	mkdir secrets
	cp -R config/dev/* secrets/
	gcloud app deploy app.yaml --project vaknl-chatbot-dev

deploy-prod:
	mkdir secrets
	cp -R config/prod/* secrets/
	gcloud app deploy --project vaknl-chatbot-prod

run-local: prepare-dev
	export FLASK_APP=main.py; \
	flask run
