# load wikidump for neo4j 3.x and convert to 5.x

# download datadump
wget https://os.unil.cloud.switch.ch/swift/v1/lts2-wikipedia/wikipedia_nrc.dump \
    -O data/wikipedia_nrc.dump

# import data
docker compose run --rm wikidump4x-neo4j neo4j-admin load \
    --force \
    --from=/var/lib/neo4j/import/wikipedia_nrc.dump
# run container
docker compose up -d wikidump4x-neo4j
# stop and start container again to upgrade the database
docker compose down
docker compose up -d wikidump4x-neo4j

# data dump
docker compose run --rm wikidump4x-neo4j neo4j-admin dump \
    --database=neo4j \
    --to=/var/lib/neo4j/import/migration/neo4j.dump
# # backup
# docker compose run --rm wikidump4x-neo4j neo4j-admin backup \
#     --database=neo4j \
#     --backup-dir=/var/lib/neo4j/import/backup/

# restore and migrate
docker compose down
docker compose run --rm wikidump5x-neo4j neo4j-admin database load neo4j \
    --from-path=/var/lib/neo4j/import/migration \
    --overwrite-destination=true \
    --verbose
docker compose run --rm wikidump5x-neo4j neo4j-admin database migrate neo4j --force-btree-indexes-to-range
docker compose up -d wikidump5x-neo4j
docker compose down
docker compose up -d wikidump5x-neo4j
