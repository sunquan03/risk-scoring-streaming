# migrations
.PHONY: migrate migrate-k8s

migrate:
	@./scripts/migrate.sh

migrate-k8s:
	@kubectl create configmap migration-sql \
		--namespace risk-pipeline \
		--from-file=migrations/ \
		--dry-run=client -o yaml | kubectl apply -f -
	@kubectl delete job migrate-tidb migrate-postgres \
		--namespace risk-pipeline --ignore-not-found
	@kubectl apply -f k8s/jobs/migrate.yaml
	@echo "waiting for migrations..."
	@kubectl wait --for=condition=complete --timeout=300s \
		job/migrate-tidb job/migrate-postgres --namespace risk-pipeline
	@kubectl logs job/migrate-tidb     --namespace risk-pipeline
	@kubectl logs job/migrate-postgres --namespace risk-pipeline
