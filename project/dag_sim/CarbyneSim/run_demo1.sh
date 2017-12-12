# 3 dags coming at different time points
java -cp "classes/:jars/json-simple-1.1.1.jar" -Djava.util.logging.config.file=logging_dagsim.properties carbyne.simulator.Main inputs/config1.json inputs/dags-input-3dags.json 2 0.1 10 true DRF CP 0.1 false true
