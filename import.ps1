$DATA_VOLUME="mac-lab-neo4j-data-vol"

# Run this command once to import the backup
docker run --interactive --tty --rm `
    --publish=7474:7474 --publish=7687:7687 `
    -v ${DATA_VOLUME}:/data `
    -v ${pwd}/source:/source `
    neo4j:4.4 `
neo4j-admin load --from=/source/contact-tracing-43.dump --database=neo4j --force

