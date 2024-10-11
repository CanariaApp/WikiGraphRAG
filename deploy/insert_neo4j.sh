docker compose down wikidump5x-neo4j
docker compose run --rm wikidump5x-neo4j bin/neo4j-admin database import full \
    --nodes=Paragraphs=/var/lib/neo4j/import/embeddings/nodes/embedding/.*.csv \
    --nodes=Pages=/var/lib/neo4j/import/embeddings/nodes/title.csv \
    --relationships=BELONGS=/var/lib/neo4j/import/embeddings/edges/embedding_pages/.*.csv \
    --relationships=LINKED=/var/lib/neo4j/import/embeddings/edges/title_title.csv \
    --array-delimiter=';' \
    --auto-skip-subsequent-headers=true \
    --skip-bad-relationships \
    --skip-duplicate-nodes \
    --overwrite-destination \
    --multiline-fields=true \
    --bad-tolerance 100 \
    --verbose
docker compose up -d wikidump5x-neo4j
